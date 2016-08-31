#demo account management program
import argparse
from nvm.fake_pmemobj import PersistentObjectPool, PersistentDict, PersistentList
import decimal
import time

#initial account creation module 

#decimal precision
decimal.getcontext().prec = 12

#top-level parser
parser = argparse.ArgumentParser()
parser.add_argument('--foo', action = 'store_true', help = 'foo help')
subparsers = parser.add_subparsers(help = 'sub-command help', dest = 'subcommand')
parser.add_argument('-f', '--filename', default='accounts.pmem', help="filename to store data in")

#create the parser for the 'accounts create' command
parser_create = subparsers.add_parser('create', description= 'account creation')
parser_create.add_argument('account', help = 'create specific type of bank account')
parser_create.add_argument('amount', help = 'establish initial balance', type=decimal.Decimal, default = decimal.Decimal('0.00'), nargs = '?')
#list account info., incl. past transactions
parser_create = subparsers.add_parser('list', description = 'individual account information display')
parser_create.add_argument('account', help = 'specify account')                         
#transfer money between accounts
parser_create = subparsers.add_parser('transfer', description = 'transer funds between accounts')
parser_create.add_argument('transferAmount', help ='quantity of money to transfer', type=decimal.Decimal, default = decimal.Decimal('0.00'))
parser_create.add_argument('pastLocation', help ='account from which funds are withdrawn')
parser_create.add_argument('futureLocation', help = 'account to which funds are deposited') 
parser_create.add_argument('memo', help = 'explanation of transfer', nargs = argparse.REMAINDER)
#withdraw money from account (check)
parser_create = subparsers.add_parser('check', description = 'withdraw money via check')
parser_create.add_argument('checkNumber', help = 'number of check')
parser_create.add_argument('account', help = 'account from which to withdraw money')
parser_create.add_argument('amount', help = 'amount withdrawn', type = decimal.Decimal, default = decimal.Decimal('0.00'))
parser_create.add_argument('memo', help = 'check memo', nargs = argparse.REMAINDER)
#deposit money into account
parser_create = subparsers.add_parser('deposit', description = 'deposit money into account')
parser_create.add_argument('account', help = 'account in which to deposit money')
parser_create.add_argument('amount', help = 'total deposit', type = decimal.Decimal, default = decimal.Decimal('0.00'))
parser_create.add_argument('memo', help = 'source of deposit', nargs = argparse.REMAINDER)
#withdraw money from account (cash)
parser_create = subparsers.add_parser('withdraw', description = 'withdraw amount of cash from account')
parser_create.add_argument('account', help = 'account from which to withdraw money')
parser_create.add_argument('amount', help = 'total withdrawl', type = decimal.Decimal, default = decimal.Decimal('0.00'))
parser_create.add_argument('memo', help = 'reason for withdrawl', nargs = argparse.REMAINDER)
args = parser.parse_args()


with PersistentObjectPool(args.filename, flag='c') as pop:
    if pop.root is None:
        pop.root = pop.new(PersistentDict)
    accounts = pop.root
    if args.subcommand == 'create':
        accounts[args.account] = [['2015-08-09', decimal.Decimal(args.amount), 'Initial account balance']]
        print("Created account '" + args.account + "'.")                       
    elif args.subcommand == 'list':
        L1 = ("Date         Amount  Balance  Memo\n"
              "----------  -------  -------  ----")
        print(L1)
        accntBalance = decimal.Decimal(0)                           
        for x in accounts[args.account]:
            accntBalance = accntBalance + x[1]
        for transaction in reversed(accounts[args.account]):
            L2= "{:<10}{:>9}{:>9}  {}".format(transaction[0], transaction[1], accntBalance, transaction[2])
            print(L2)
            accntBalance = accntBalance - transaction[1]
    elif args.subcommand == 'transfer':
        s= " "
        memo = s.join(args.memo)
        memo = memo[0].upper() + memo[1:]
        accounts[args.pastLocation].append(['2015-08-09', -decimal.Decimal(args.transferAmount), memo])         
        accounts[args.futureLocation].append(['2015-08-09', decimal.Decimal(args.transferAmount), memo])
        pBalance = decimal.Decimal(0)
        for transaction in accounts[args.pastLocation]:
            pBalance = pBalance + transaction[1]
        fBalance = decimal.Decimal(0)
        for transaction in accounts[args.futureLocation]:
            fBalance = fBalance + transaction[1]
        print("Transferred {} from {} (new balance {}) to {} (new balance {})".format(args.transferAmount, args.pastLocation, str(pBalance), args.futureLocation, str(fBalance)))
    elif args.subcommand == 'check':
        c = " "
        memo = c.join(args.memo)
        memo = memo[0].upper() + memo[1:]
        accounts[args.account].append(['2015-08-09', -decimal.Decimal(args.amount), "Check {}: {}".format(args.checkNumber, memo)])
        newBalance = decimal.Decimal(0)
        for transaction in accounts[args.account]:
            newBalance = newBalance + transaction[1]
        print("Check {} for {} debited from {} (new balance {})".format(args.checkNumber, args.amount, args.account, newBalance))
    elif args.subcommand == 'deposit':
        c = " "
        memo = c.join(args.memo)
        memo = memo[0].upper() + memo[1:]
        accounts[args.account].append(['2015-08-09', decimal.Decimal(args.amount), memo])
        newBalance = decimal.Decimal(0)
        for transaction in accounts[args.account]:
            newBalance = newBalance + transaction[1]
        print("{} added to {} (new balance {})".format(args.amount, args.account, newBalance))
    elif args.subcommand == 'withdraw':
        c = " "
        memo = c.join(args.memo)
        memo = memo[0].upper() + memo[1:]
        accounts[args.account].append(['2015-08-09', -decimal.Decimal(args.amount), memo])
        newBalance = decimal.Decimal(0)
        for transaction in accounts[args.account]:
            newBalance = newBalance + transaction[1]
        print("{} withdrawn from {} (new balance {})".format(args.amount, args.account, newBalance))
    else:
        #check for accounts
        if accounts:
            #25 character spaces
            m1= ("Account            Balance\n"
                 "-------            -------")
            print(m1)
            accntTotal = decimal.Decimal(0)
            for specificAccount in sorted(accounts):
                balance= decimal.Decimal(0.0)
                for transaction in accounts[specificAccount]:
                    balance = balance + transaction[1]
                accntInfo = "{:<20}{:>6}".format(specificAccount, balance)
                print(accntInfo)
                accntTotal = accntTotal + balance
            m2=("                   _______\n"
                "    Net Worth:      {:>6.2f}").format(accntTotal)
            print (m2)
        #print message if no accounts exist
        else:
            print("No accounts currently exist.  Add an account using 'account create'.")
