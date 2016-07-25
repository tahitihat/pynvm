import argparse
import curses
import sys
from contextlib import contextmanager
from random import randrange, randint
from time import sleep

from nvm.pmemobj import PersistentObjectPool, PersistentList

import logging
#logging.basicConfig(level=logging.DEBUG)

GAME_WIDTH = 50
GAME_HEIGHT = 25

ALIENS_ROW = 4
ALIENS_COL = 18

STEP = 50/1000000

PLAYER_Y = GAME_HEIGHT - 1

MAX_GSTATE_TIMER = 10000
MIN_GSTATE_TIMER = 5000
MAX_ALIEN_TIMER = 1000
MAX_PLAYER_TIMER = 1000
MAX_BULLET_TIMER = 500
MAX_STAR1_TIMER = 200
MAX_STAR2_TIMER = 100

C_UNKNOWN = 0
C_PLAYER = 1
C_ALIEN = 2
C_BULLET = 3
C_STAR = 4
C_INTRO = 5

# When we have a working PersistentDict we can use namspaces instead of
# this named-index hack.
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
        q = ord('q')
        sp = ord(' ')
        while exit not in (q, sp):
            exit = self.screen.getch()
            self.screen.erase()
            self.draw_border()
            if not self.root[ROOT_STARS]:
                self.create_stars()
            self.process_stars()
            self.draw_title()
            sleep(STEP)
            self.screen.refresh()
        return exit == q

    def run(self):
        exit = self.intro_loop()
        if exit:
            return
        # game_loop

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
