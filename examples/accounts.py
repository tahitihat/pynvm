#demo account management program
import argparse
from nvm.fake_pmemobj import PersistentObjectPool, PersistentDict, PersistentList
import decimal 
#initial account creation module 

#decimal precision
decimal.getcontext().prec = 2

#top-level parser
parser = argparse.ArgumentParser()
parser.add_argument('--foo', action = 'store_true', help = 'foo help')
subparsers = parser.add_subparsers(help = 'sub-command help', dest = 'subcommand')
parser.add_argument('-f', '--filename', default='accounts.pmem', help="filename to store data in")

#create the parser for the 'accounts create' command
parser_create = subparsers.add_parser('create', description= 'account creation')
#(specify type of account)
parser_create.add_argument('account', help = 'create specific type of bank account')
#(establish initial balance in accnt)
parser_create.add_argument('amount', help = "establish initial balance", type=decimal.Decimal, default = decimal.Decimal('0.00'), nargs = '?')
args_create = parser.parse_args()

#temporary
#print(args_create)

with PersistentObjectPool(args_create.filename, flag='c') as pop:
    if pop.root is None:
        pop.root = pop.new(PersistentDict)
    accounts = pop.root
    if args_create.subcommand == 'create':
        accounts[args_create.account] = args_create.amount
        print("Created account '" + args_create.account + "'.")
    #if subcommand == 'transfer": 
    else:
        #check for accounts
        if accounts:
            m1= ("Account           Balance\n"
                 "-------           -------")
            print(m1)
            accntTotal = 0
            for specificAccount in accounts:
                accntInfo = "{}             {}".format(specificAccount, accounts[specificAccount])
                print(accntInfo)
                accntTotal = accntTotal + accounts[specificAccount]
            m2=("                         _______\n"
                "    Net Worth:           {}").format(accntTotal)
            print(m2)
        #print message if no accounts exist
        else:
            print("No accounts currently exist.  Add an account using 'account create'.")
