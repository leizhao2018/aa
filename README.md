# check-the-data

all flowchart.html:https://drive.google.com/drive/folders/1WyvMXN-6E621OeJ1o8cLeW5F3BpIq_cK
The function of check reformat data:
1. Check the name of the data files, the name have the "Lowell-SN", and the lowell-sn have 4 alphabet;
   sometimes, the name lack of two alphabet, so, we need to complete it.
2. Some longitude and latitude data is not the standard data(standard data is like 1234.1234). 
   some have the alphabet that present the direction,we need delete this alphabet.
   some data is not 4 numbers in the front of the point.we need to keep 4 numbers in the front of the point.
3. Check the head data, whether the vessel number is right, if not,fix it
   whether the vessel name is exist or the name is right, if not, insert it or fix it.
4. Check the data is in standard format(the standard data have 6 columns).
   if not, it always lack the column of the "HEADING",so insert this column.
     
The function of match_raw_tele:
1. Get the start time and the end time of each raw folder data.
2. Match the file and telementy. we can known how many file send to the satallite.
3. Caculate the numbers of  succeesful matchs
4. Caculate the numbers of transmissions during time of raw folder
5. Caculate the percent of successful matching in raw folder and transmission

