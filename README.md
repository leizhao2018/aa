# check-the-data
The function of check data:
1. Check the name of the data files, the name have the "Lowell-SN", and the lowell-sn have 4 alphabet;
   sometimes, the name lack of two alphabet, so, we need to complete it.
2. Some longitude and latitude data is not the standard data(standard data is like 1234.1234). 
   some have the alphabet that present the direction,we need delete this alphabet.
   some data is not 4 numbers in the front of the point.we need to keep 4 numbers in the front of the point.
3. Check the head data, whether the vessel number is right, if not,fix it
   whether the vessel name is exist or the name is right, if not, insert it or fix it.
4. Check the data is in standard format(the standard data have 6 columns).
   if not, it always lack the column of the "HEADING",so insert this column.
