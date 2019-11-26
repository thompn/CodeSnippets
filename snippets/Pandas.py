# clean headers of pandas dataframes
def clean_pandas(df):
    df.columns = df.columns\
		# remove leading and trailing space
		.str.strip()\
                #make lower case
                .str.lower()\
                # replace spaces with '_'
                .str.replace(' ', '_')\
                # remove open parenthesis
                .str.replace('(', '')\
                # remove close parenthesis
                .str.replace(')', '')

