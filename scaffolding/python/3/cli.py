# doc - https://docs.python.org/3.3/library/argparse.html#nargs
import argparse

VERSION = 1.0

def process_args():
	parser = argparse.ArgumentParser(description='CLI scaffolding.')
	parser.add_argument('--version', action='version', version=str(VERSION))
	parser.add_argument('-f', '--flag', action='store_true',
		                help='flag desc')
	parser.add_argument('args', metavar='N', type=int, nargs=1,
	                    help='args desc')
	parser.add_argument('--option', dest='option1_dest',
		                help='option1 desc')

	args = parser.parse_args()	
	print("args={}".format(args))

	if hasattr(args, 'option1'):
		print("args.option1={}".format(args.option1))	

def main():
	print("Hello world.")

if __name__ == "__main__":
	process_args()
	main()