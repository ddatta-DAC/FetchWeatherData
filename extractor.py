import mongoDBI
from datetime import date
from datetime import timedelta
import pandas as pd

database_name = 'Weather_date_complete'

output_file = 'weather.csv'

# ----------- #
# NYC Lat long
# indices longitude 96, latitude 49
# ----------- #

columns = ['rel_hum' , 'sp_hum' , 'abs_hum' , 'u_wind', 'v_wind', 'temp']
dbi_search = mongoDBI.mongoDBI (db_name=database_name)
feature = "rel_hum"

num_rec_per_day = 4
key_label = "date_idx"
key_contents = "2009-01-03_1"
value_label = "data" 


start_date = date(2002,6,1)
end_date = date(2002,10,31)
time_increment = timedelta(days=1)

# Store data in data frame
df =  pd.DataFrame(columns = columns )

current_date = start_date
# Loop through each date
while True:
	
	# Loop through 4 records
	for idx in range(1, num_rec_per_day+1):
		key_contents = str(current_date) + '_' +  str(idx)
		cur_res = {}
		skip = False
		# Loop though the columns
		for c in columns :
			feature =  c
			print  feature , key_label , key_contents 

			res = dbi_search.find (
					feature, 
					key_label, 
					key_contents, 
					value_label
					)
			# print res.shape
			try :
				val = res[49,96] 
				cur_res[c] = val
			except :
				skip = True
				pass

		if skip == False :
			df = df.append(cur_res,ignore_index=True)
										
	current_date = current_date + time_increment 
	if current_date > end_date :
		break
	

print df
df.to_csv(output_file)




	  

