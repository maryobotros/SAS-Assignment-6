***DRAFT - CONFIDENTIAL ATTORNEY WORK PRODUCT***;
/*footnote "DRAFT-CONFIDENTIAL ATTORNEY WORK PRODUCT";*/

/*SAS Assignment 6*/
/*Author: Maryo Botros*/

/*------------------------ Question #1 ------------------------*/
data hr_transactions;
	format
		empid $char20.
		first $char20.
		last $char20.
		rectype $char20.
		effdate mmddyy10.
		position $char20.;

	informat 
		empid $char20.
		first $char20.
		last $char20.
		rectype $char20.
		effdate mmddyy10.
		position $char20.;

	infile "/mount/resdata/2. SAS Assignments & Training Data/Analyst Training Folders/Maryo Botros/Assignment 6/Munchies - HR Transactions.csv"
		lrecl=32767
		firstobs=2
		termstr=crlf
		dlm=','
		missover
		dsd;

	input 
		empid
		first
		last
		rectype
		effdate
		position;
run;

data location_data;
	infile "/mount/resdata/2. SAS Assignments & Training Data/Analyst Training Folders/Maryo Botros/Assignment 6/Munchies - Location Data.csv"
		lrecl=32767
		firstobs=2
		termstr=crlf
		dlm=','
		missover
		dsd;

	format
		empid $char20.
		store best.
		effdate_ $50.;

	informat
		empid $char20.
		store best.
		effdate_ $50.;
	input 
		empid
		store
		effdate_;
run;

data test;
	set location_data;

format mdy mmddyy10.;	
month = scan(effdate_, 1, "/")+0;
day = scan(effdate_, -2, "/")+0;
yr = scan(effdate_, -1, "/")+0;
mdy = mdy(month, day, yr);

run;


	 



/*Freedom to set up the case folder the way thar you want
*/


