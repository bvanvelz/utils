import re

#
# Config
#

FILE = './data/20200726_weekly_housing_market_data_most_recent.tsv'

TABLE = 'redfin_data'

OUTPUT_DIR = './output'

def main():
	counter = 0

	column_names = []
	column_data_types = []
	data_types_incomplete = True  # boolean

	fh_r = open(FILE)
	fh_inserts = open('{}/{}_inserts.sql'.format(OUTPUT_DIR, TABLE), "w")

	# determine columns and data types
	for line in fh_r:
		# transform line
		line = line.rstrip()
		if len(line) == 0:
			continue

		counter += 1

		line_split = line.split("\t")

		if counter == 1:
			column_names = line_split
		elif counter > 1 and data_types_incomplete:
			column_data_types = get_data_types(column_data_types, line_split)
			if _contains_no_nones(column_data_types):
				data_types_incomplete = False
				write_create_table(column_names, column_data_types)
				break

	fh_r.close()


	counter = 0
	fh_r = open(FILE)
	for line in fh_r:
		line = line.rstrip()
		if len(line) == 0:
			continue
		counter += 1

		line_split = line.split("\t")

		if counter > 1:
			write_insert(fh_inserts, column_data_types, line_split)

		#if counter == 10000:
		#	break


	fh_r.close()
	fh_inserts.close()


def _contains_no_nones(a):
	for d in a:
		if d is None:
			return False
	return True


def get_data_types(data_types, line_split):

	i = 0

	for d in line_split:

		if i <= len(data_types) - 1 :
			# skip already defined data types
			if data_types[i]:
				i += 1
				continue
			else:
				dt = get_data_type(d)
				data_types[i] = dt
		else:
			dt = get_data_type(d)
			data_types.append(dt)

		i += 1

	return data_types


def get_data_type(value):
	if value == '':
		return None

	try:
		int(value)
	 	return 'INT'
	except Exception:
		pass

	try:
		float(value)
		return 'FLOAT'
	except Exception:
		pass

	if re.match('^\d\d\d\d-\d\d-\d\d$', value):
		return 'DATE'

	if re.match('^\d\d\d\d-\d\d-\d\d.\d\d:\d\d', value):
		return 'TIMESTAMP'

	if re.search(r'\w', value):
		return 'VARCHAR(255)'

	raise Exception("Unknown data type for value: {}".format(value))


def write_create_table(column_names, data_types):
	fh_w = open('{}/{}_create_table.ddl'.format(OUTPUT_DIR, TABLE), "w")

	fh_w.write("CREATE TABLE {} (\n".format(TABLE))

	col_w_type = []
	i = 0
	for column in column_names:
		col_w_type.append("\t{} {}".format(column, data_types[i]))
		i += 1

	fh_w.write(",\n".join(col_w_type))
	fh_w.write("\n);")


def write_insert(fh_w, data_types, line_split):
	i = 0
	line_split_w_qoutes = []
	for d in line_split:
		if d is None or d == '':
			line_split_w_qoutes.append('NULL')
		elif data_types[i].lower() in ('int', 'float'):
			line_split_w_qoutes.append(d)
		elif data_types[i].lower() in ('timestamp', 'date', 'text') or re.match('varchar', data_types[i].lower()):
			# escape qoutes
			d = re.sub("'", "''", d)
			line_split_w_qoutes.append("'{}'".format(d))
		else:
			raise Exception("Unrecognized data type: '{}'".format(data_types[i]))
		i += 1

	fh_w.write("INSERT INTO {} VALUES({});\n".format(TABLE, ", ".join(line_split_w_qoutes)))


if __name__=="__main__":
	main()
