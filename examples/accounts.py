#demo account management program
import argparse
from nvm.fake_pmemobj import PersistentObjectPool, PersistentDict, PersistentList
#initial account creation module 


#top-level parser
parser = argparse.ArgumentParser()
parser.add_argument('--foo', action = 'store_true', help = 'foo help')
subparsers = parser.add_subparsers(help = 'sub-command help', dest = 'subcommand')
parser.add_argument('-f', '--filename', default='accounts.pmem', help="filename to store data in")

#create the parser for the 'account create' command
parser_create = subparsers.add_parser('create', description= 'account creation')
#(specify type of account)
parser_create.add_argument('account', help = 'create specific type of bank account')
#(establish initial balance in accnt)
parser_create.add_argument('amount', help = "establish initial balance", type=float, default = 0, nargs = '?')
args_create = parser.parse_args()

#temporary
#print(args_create)

#***use name of acct to create dictionary

with PersistentObjectPool(args_create.filename, flag='c') as pop:
    if pop.root is None:
        pop.root = pop.new(PersistentDict)
    accounts = pop.root
    if args_create.subcommand == 'create':
        accounts[args_create.account] = args_create.amount
        print(accounts)
    #if subcommand = 'transfer": 
    else:
        #check for accounts
        #if ..
        print()
        print("No accounts currently exist.  Add an account using 'account create'.")
    
