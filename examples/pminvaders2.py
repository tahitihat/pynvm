import argparse
import curses
import sys
from contextlib import contextmanager
from random import randrange, randint
from time import sleep

from nvm.pmemobj import PersistentObjectPool, PersistentList

import logging
#logging.basicConfig(level=logging.DEBUG)

# We're slow, so shorten the timers and increase the delay.  Even with this
# the pmem version is slower than the non-pmem version.
STEP = 50000/1000000
MAX_GSTATE_TIMER = 100
MIN_GSTATE_TIMER = 50
MAX_ALIEN_TIMER = 10
MAX_PLAYER_TIMER = 10
MAX_BULLET_TIMER = 5
MAX_STAR1_TIMER = 2
MAX_STAR2_TIMER = 1
ALIEN_TIMER_LEVEL_FACTOR = 1

GAME_WIDTH = 50
GAME_HEIGHT = 25

ALIENS_ROW = 4
ALIENS_COL = 18

PLAYER_Y = GAME_HEIGHT - 1

C_UNKNOWN = 0
C_PLAYER = 1
C_ALIEN = 2
C_BULLET = 3
C_STAR = 4
C_INTRO = 5

EVENT_PLAYER_KILLED = 0
EVENT_ALIENS_KILLED = 1
EVENT_BOUNCE = 2

CH_Q = ord('q')
CH_SP = ord(' ')
CH_O = ord('o')
CH_P = ord('p')

# When we have a working PersistentObject we can use objects instead of
# this list-with-named-indexes hack.
ROOT_STATE = 0
ROOT_PLAYER = 1
ROOT_ALIENS = 2
ROOT_BULLETS = 3
ROOT_STARS = 4
ROOT_LEN = 5

STATE_TIMER = 0
STATE_SCORE = 1
STATE_HIGH_SCORE = 2
STATE_LEVEL = 3
STATE_NEW_LEVEL = 4
STATE_DX = 5
STATE_DY = 6

PLAYER_X = 0
PLAYER_TIMER = 1

BULLET_X = 0
BULLET_Y = 1
BULLET_TIMER = 2

ALIEN_X = 0
ALIEN_Y = 1

STAR_X = 0
STAR_Y = 1
STAR_C = 2
STAR_TIMER = 3
# End of named-index hack.

parser = argparse.ArgumentParser()
parser.add_argument('fn', help="Persistemt memory game file")
parser.add_argument('--no-pmem', action='store_true',
                    help="Use dummy PersistentObjectPool instead of real one")

class DummyPersistentObjectPool:
    def __init__(self, *args):
        self.root = None
        pass
    def new(self, typ, *args, **kw):
        if typ == PersistentList:
            return list(*args, **kw)
    @contextmanager
    def transaction(self):
        yield None
    def close(self):
        pass
    def __enter__(self):
        return self
    def __exit__(self, *args):
        pass


class PMInvaders2(object):

    closed = True

    def __init__(self, pop):
        self.pop = pop
        # curses init
        screen = self.screen = curses.initscr()
        self.closed = False
        curses.start_color()
        curses.init_pair(C_PLAYER, curses.COLOR_GREEN, curses.COLOR_BLACK)
        curses.init_pair(C_ALIEN, curses.COLOR_RED, curses.COLOR_BLACK)
        curses.init_pair(C_BULLET, curses.COLOR_YELLOW, curses.COLOR_BLACK)
        curses.init_pair(C_STAR, curses.COLOR_WHITE, curses.COLOR_BLACK)
        curses.init_pair(C_INTRO, curses.COLOR_BLUE, curses.COLOR_BLACK)
        screen.nodelay(True)
        curses.curs_set(0)
        screen.keypad(True)
        # Game init
        if pop.root is None:
            pop.root = pop.new(PersistentList, [None] * ROOT_LEN)
        if pop.root[ROOT_STATE] is None:
            pop.root[ROOT_STATE] = self.pop.new(PersistentList,
                [1, 0, 0, 0, 1, 1, 0])
        if pop.root[ROOT_PLAYER] is None:
            pop.root[ROOT_PLAYER] = self.pop.new(PersistentList,
                [GAME_WIDTH // 2, 1])
        if pop.root[ROOT_ALIENS] is None:
            pop.root[ROOT_ALIENS] = self.pop.new(PersistentList)
        if pop.root[ROOT_BULLETS] is None:
            pop.root[ROOT_BULLETS] = self.pop.new(PersistentList)
        if pop.root[ROOT_STARS] is None:
            pop.root[ROOT_STARS] = self.pop.new(PersistentList)
        self.root = pop.root

    def close(self):
        curses.endwin()
        self.closed = True

    def __del__(self):
        if not self.closed:
            self.close()

    def draw_border(self):
        screen = self.screen
        for x in range(GAME_WIDTH+1):
            screen.addch(0, x, curses.ACS_HLINE)
            screen.addch(GAME_HEIGHT, x, curses.ACS_HLINE)
        for y in range(GAME_HEIGHT+1):
            screen.addch(y, 0, curses.ACS_VLINE)
            screen.addch(y, GAME_WIDTH, curses.ACS_VLINE)
        screen.addch(0, 0, curses.ACS_ULCORNER)
        screen.addch(GAME_HEIGHT, 0, curses.ACS_LLCORNER)
        screen.addch(0, GAME_WIDTH, curses.ACS_URCORNER)
        screen.addch(GAME_HEIGHT, GAME_WIDTH, curses.ACS_LRCORNER)

    def create_star(self, x, y):
        c = '*' if randint(0, 1) else '.'
        timer = MAX_STAR1_TIMER if c == '.' else MAX_STAR2_TIMER
        return self.pop.new(PersistentList, [x, y, c, timer])

    def create_stars(self):
        # C version prepends to list; I'm appending so list is reversed.  Our
        # append is as atomic as the C code's linked list pointer assignment.
        for x in range(1, GAME_WIDTH):
            if randrange(0, 100) < 4:
                self.root[ROOT_STARS].append(self.create_star(x, 1))

    def draw_star(self, star):
        self.screen.addch(star[STAR_Y], star[STAR_X], star[STAR_C],
                          curses.color_pair(C_STAR))

    def process_stars(self):
        new_line = False
        with self.pop.transaction():
            stars = self.root[ROOT_STARS]
            for star in list(stars):
                star[STAR_TIMER] -= 1
                if not star[STAR_TIMER]:
                    if star[STAR_C] == '.':
                        star[STAR_TIMER] = MAX_STAR1_TIMER
                        new_line = True
                    else:
                        star[STAR_TIMER] = MAX_STAR2_TIMER
                    star[STAR_Y] += 1
                self.draw_star(star)
                if star[STAR_Y] >= GAME_HEIGHT:
                    stars.remove(star)
            if new_line:
                self.create_stars()

    def printw(self, y, x, string):
        for i in range(x, x + len(string)):
            self.screen.addch(y, i, string[i - x])

    def draw_title(self):
        screen = self.screen
        x = (GAME_WIDTH -40) // 2
        y = GAME_HEIGHT // 2 - 2
        screen.attron(curses.color_pair(C_INTRO))
        self.printw(y + 0, x, "#### #   # ### #   # #   #     ###   ###")
        self.printw(y + 1, x, "#  # ## ##  #  ##  # #   #       #   # #")
        self.printw(y + 2, x, "#### # # #  #  # # #  # #      ###   # #")
        self.printw(y + 3, x, "#    # # #  #  #  ##  # #      #     # #")
        self.printw(y + 4, x, "#    #   # ### #   #   #       ### # ###")
        screen.attroff(curses.color_pair(C_INTRO))
        self.printw(y + 6, x, "      Press 'space' to resume           ")
        self.printw(y + 7, x, "      Press 'q' to quit                 ")

    def intro_loop(self):
        exit = None
        while exit not in (CH_Q, CH_SP):
            exit = self.screen.getch()
            self.screen.erase()
            self.draw_border()
            if not self.root[ROOT_STARS]:
                self.create_stars()
            self.process_stars()
            self.draw_title()
            sleep(STEP)
            self.screen.refresh()
        return exit == CH_Q

    def draw_score(self):
        state = self.root[ROOT_STATE]
        self.printw(1, 1, "Level: {:5} Score: {} | {}".format(
                    state[STATE_LEVEL],
                    state[STATE_SCORE],
                    state[STATE_HIGH_SCORE]))

    def remove_aliens(self):
        self.root[ROOT_ALIENS].clear()

    def create_aliens(self):
        aliens = self.root[ROOT_ALIENS]
        for x in range(ALIENS_COL):
            for y in range(ALIENS_ROW):
                aliens.append(self.pop.new(PersistentList,
                              [GAME_WIDTH // 2 - ALIENS_COL + x * 2, y + 3]))

    def new_level(self):
        with self.pop.transaction():
            self.remove_aliens()
            self.create_aliens()
            state = self.root[ROOT_STATE]
            if state[STATE_NEW_LEVEL] > 0 or state[STATE_LEVEL] > 1:
                state[STATE_LEVEL] += state[STATE_NEW_LEVEL]
            state[STATE_NEW_LEVEL] = 0
            state[STATE_DX] = 1
            state[STATE_DY] = 0
            state[STATE_TIMER] = (MAX_ALIEN_TIMER
                                      - ALIEN_TIMER_LEVEL_FACTOR
                                      * (state[STATE_LEVEL] - 1))

    def update_score(self, delta):
        state = self.root[ROOT_STATE]
        if delta < 0 and not state[STATE_SCORE]:
            return
        state[STATE_SCORE] += delta
        if state[STATE_SCORE] < 0:
            state[STATE_SCORE] = 0
        if state[STATE_SCORE] > state[STATE_HIGH_SCORE]:
            state[STATE_HIGH_SCORE] = state[STATE_SCORE]

    def move_aliens(self):
        aliens = self.root[ROOT_ALIENS]
        player = self.root[ROOT_PLAYER]
        state = self.root[ROOT_STATE]
        dx = state[STATE_DX]
        dy = state[STATE_DY]
        if not aliens:
            return EVENT_ALIENS_KILLED
        event = None
        for alien in aliens:
            if dy:
                alien[ALIEN_Y] += dy
            if dx:
                alien[ALIEN_X] += dx
            if alien[ALIEN_Y] >= PLAYER_Y:
                event = EVENT_PLAYER_KILLED
            elif (dy == 0
                  and alien[ALIEN_X] >= GAME_WIDTH - 2
                  or alien[ALIEN_X] <= 2):
                event = EVENT_BOUNCE
        return event

    def process_aliens(self):
        state = self.root[ROOT_STATE]
        with self.pop.transaction():
            state[STATE_TIMER] -= 1
            if not state[STATE_TIMER]:
                state[STATE_TIMER] = (MAX_ALIEN_TIMER
                                        - ALIEN_TIMER_LEVEL_FACTOR
                                        * (state[STATE_LEVEL] - 1))
                event = self.move_aliens()
                if event == EVENT_ALIENS_KILLED:
                    state[STATE_NEW_LEVEL] = 1
                elif event == EVENT_PLAYER_KILLED:
                    curses.flash()
                    curses.beep()
                    state[STATE_NEW_LEVEL] = -1
                    self.update_score(-100)
                elif event == EVENT_BOUNCE:
                    state[STATE_DY] = 1
                    state[STATE_DX] = -state[STATE_DX]
                elif state[STATE_DY]:
                    state[STATE_DY] = 0
        for alien in self.root[ROOT_ALIENS]:
            self.screen.addch(alien[ALIEN_Y], alien[ALIEN_X],
                              curses.ACS_DIAMOND, curses.color_pair(C_ALIEN))

    def process_collision(self, bullet):
        aliens = self.root[ROOT_ALIENS]
        with self.pop.transaction():
            for alien in list(aliens):
                if (bullet[BULLET_X] == alien[ALIEN_X]
                        and bullet[BULLET_Y] == alien[ALIEN_Y]):
                    self.update_score(1)
                    aliens.remove(alien)
                    return True
        return False

    def process_bullets(self):
        with self.pop.transaction():
            for bullet in list(self.root[ROOT_BULLETS]):
                bullet[BULLET_TIMER] -= 1
                if not bullet[BULLET_TIMER]:
                    bullet[BULLET_TIMER] = MAX_BULLET_TIMER
                    bullet[BULLET_Y] -= 1
                self.screen.addch(bullet[BULLET_Y], bullet[BULLET_X],
                                  curses.ACS_BULLET,
                                  curses.color_pair(C_BULLET))
                if bullet[BULLET_Y] <= 0 or self.process_collision(bullet):
                    self.root[ROOT_BULLETS].remove(bullet)

    def process_player(self, ch):
        with self.pop.transaction():
            player = self.root[ROOT_PLAYER]
            player[PLAYER_TIMER] -= 1
            if ch in (CH_O, curses.KEY_LEFT):
                dstx = player[PLAYER_X] - 1
                if dstx:
                    player[PLAYER_X] = dstx
            elif ch in (CH_P, curses.KEY_RIGHT):
                dstx = player[PLAYER_X] + 1
                if dstx != GAME_WIDTH:
                    player[PLAYER_X] = dstx
            elif ch == CH_SP and player[PLAYER_TIMER] <= 0:
                player[PLAYER_TIMER] = MAX_PLAYER_TIMER
                self.root[ROOT_BULLETS].append(self.pop.new(PersistentList,
                    [player[PLAYER_X], PLAYER_Y-1, 1]))
        self.screen.addch(PLAYER_Y, player[PLAYER_X],
                          curses.ACS_DIAMOND,
                          curses.color_pair(C_PLAYER))

    def game_loop(self):
        ch = None
        state = self.root[ROOT_STATE]
        while ch != CH_Q:
            ch = self.screen.getch()
            self.screen.erase()
            self.draw_score()
            self.draw_border()
            with self.pop.transaction():
                if state[STATE_NEW_LEVEL]:
                    self.new_level()
                self.process_aliens()
                self.process_bullets()
                self.process_player(ch)
            sleep(STEP)
            self.screen.refresh()

    def run(self):
        exit = self.intro_loop()
        if exit:
            return
        self.game_loop()

if __name__ == '__main__':
    args = parser.parse_args()
    if args.no_pmem:
        PersistentObjectPool = DummyPersistentObjectPool
    pop = PersistentObjectPool(args.fn, flag='c')
    g = PMInvaders2(pop)
    try:
        g.run()
    finally:
        g.close()
        pop.close()
