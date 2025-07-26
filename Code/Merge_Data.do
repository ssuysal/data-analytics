ssc inst groups
ssc inst moss
ssc inst reclink
ssc inst jarowinkler
ssc inst unique

* <<REPLACE_ME>> with the correct directory
cd "H:"

tempfile base temp

*** Begin by loading in classified speech data:
import delimited using "Final/Prediction.csv", varnames(1) delimiter(",") bindquote(strict) clear
* drop these two duplicates found in the original dataset, otherwise errors occur below
* basepk=4357477 (found in both Speeches2000_2.xlsx and Speeches2000_3.xlsx)
* basepk=4599915 (found in both Speeches2010_1.xlsx and Speeches2010_2.xlsx)
duplicates drop basepk, force

save `temp', replace

*** load original data in
forvalues year = 1980(10)2010 {
	forvalues step = 0(1)3 {
		import excel using Input/Speeches`year'_`step'.xlsx, first clear
		drop hid opid speechtext speakeroldname speakerposition speakerriding speakerparty speakerurl subtopic subsubtopic

		cap destring basepk, replace force
		merge 1:1 basepk using `temp'
		drop if _merge==1
		drop _merge

		save `temp', replace
	}
}

compress
count

tab maintopic // unreadable

groups maintopic, order(h) select(10)	 // ten most frequent topics

*** Can we merge this to the MP_ID data?
* define a program to "clean strings"
cap program drop clean_string
program define clean_string
	syntax, cleanvar(str)
	replace `cleanvar' = subinstr(`cleanvar',"Ã©","e",.)
	replace `cleanvar' = subinstr(`cleanvar',"Ã®","i",.)
	replace `cleanvar' = subinstr(`cleanvar',"Ãª","e",.)
	replace `cleanvar' = subinstr(`cleanvar',"Ã®","i",.)
	replace `cleanvar' = subinstr(`cleanvar',"Ã¯","i",.)
	replace `cleanvar' = subinstr(`cleanvar',"Ã¨","e",.)
	replace `cleanvar' = subinstr(`cleanvar',"Ã´","e",.)
	replace `cleanvar' = subinstr(`cleanvar',"Ã‰","E",.)
	replace `cleanvar' = subinstr(`cleanvar',"Ã«","e",.)
	replace `cleanvar' = subinstr(`cleanvar',"é","e",.)
	replace `cleanvar' = subinstr(`cleanvar',"è","e",.)
	replace `cleanvar' = subinstr(`cleanvar',"É","e",.)
	replace `cleanvar' = subinstr(`cleanvar',"È","e",.)
	replace `cleanvar' = subinstr(`cleanvar',"ë","e",.)
	replace `cleanvar' = subinstr(`cleanvar',"î","i",.)
	replace `cleanvar' = subinstr(`cleanvar',"ï","i",.)
	replace `cleanvar' = subinstr(`cleanvar',"ç","c",.)
	replace `cleanvar' = subinstr(`cleanvar',"ô","o",.)
	replace `cleanvar' = subinstr(`cleanvar',"ê","o",.)
	replace `cleanvar' = subinstr(`cleanvar',"  "," ",.)
	replace `cleanvar' = subinstr(`cleanvar',".","",.)
	replace `cleanvar' = subinstr(`cleanvar',"Ã§","c",.)


	* erase parenthesis
	cap drop _count
	cap drop _match*
	cap drop _po*
	moss `cleanvar', match("\((.*)\)") regex
	qui su _count
	if `r(max)'>0 {
		replace `cleanvar' = subinstr(`cleanvar',_match1,"",.)
	}
	replace `cleanvar' = subinstr(`cleanvar',"(","",.)
	replace `cleanvar' = subinstr(`cleanvar',")","",.)
	replace `cleanvar' = subinstr(`cleanvar',"  "," ",.)

	replace `cleanvar' = trim(`cleanvar')
	replace `cleanvar' = lower(`cleanvar')
	cap drop _count _match* _po*

	cap drop hhh
	gen hhh = `cleanvar'
	split hhh, p(" ")
	gen lastname = ""
	forvalues n = 10(-1)2 {
		cap replace lastname = hhh`n' if lastname==""
		}
	gen firstname = subinstr(hhh,lastname,"",.)
	replace firstname = trim(firstname)
	replace lastname = trim(lastname)
	cap drop hhh*

end

* Prepare speech data:
br speakername
gen MP_Name = speakername
clean_string, cleanvar(MP_Name)

* save full data:
save Final/SpeechNames.dta, replace


*** Does not really work this way, we need to get a measure of how long they were active in order to match them better!
********************************************************************************
*** Version two:
use Final/SpeechNames.dta,clear

*** Now create smaller data only containing names
*** One realizes that the pid is unique per name:
keep pid MP_Name firstname lastname
duplicates drop pid, force
sort pid
gen Using_ID = _n
count
tempfile temp
save `temp', replace

* Prepare MP data, only focus on the MP_ID (which is constant across names):
import delimited using "Final/MPData.csv", varnames(1) delimiter(";") bindquote(strict) clear

* Change: we need to rename as variables are converted to lowercase
foreach var of varlist _all {
    local label : variable label `var'
    if "`label'" != "" & "`label'" != "`var'" {
        capture rename `var' `label'
    }
}


keep MP_ID Name MP
keep if MP!=""
gen help = MP
split help, p("(" " - ")
split help2, p("/")

* Change: get year parts by using the first four digits
gen year_start = real(substr(help21, 1, 4))
split help3 , p("/")
gen year_end = real(substr(help31, 1, 4))
drop help*
collapse (min) year_start (max) year_end, by(MP_ID Name)

* Change: now we can drop useless candidates:
drop if year_end<1980
drop if year_start>2020

duplicates drop MP_ID, force

gen MP_Name = Name

drop Name*
clean_string, cleanvar(MP_Name)
sort MP_ID
gen Master_ID = _n


reclink firstname lastname MP_Name  using `temp', ///
		idmaster(Master_ID) idusing(Using_ID) gen(simil)  wmatch(5 15 10)


sort simil
count
count if simil>0.95&simil!=.
count if simil>0.90&simil!=.
count if simil>0.85&simil!=.
drop if simil==.

jarowinkler firstname Ufirstname, gen(simil_fn)
jarowinkler lastname Ulastname, gen(simil_ln)
count
duplicates report MP_ID
duplicates report pid	// Problem
duplicates tag pid, gen(tag)

sort pid MP_ID
br lastname Ulastname firstname Ufirstname pid MP_ID if tag!=0
*we can't get rid of John Alexander Macdonald... more work is done. Not here
drop if MP_ID==3854|MP_ID==13187
*Change: drop duplicate rows
duplicates drop MP_ID, force
duplicates drop pid, force

drop if simil<0.9
unique MP_ID
unique pid
keep MP_ID pid
save Final/Transfer_Speech_MP.dta, replace

********************************************************************************

use Final/Transfer_Speech_MP.dta, clear
merge 1:m pid using SpeechNames.dta
tab _merge
keep if _merge==3
drop _merge

* Change: create a dataset that associates each MP_ID with its MP_Name
preserve
keep MP_ID MP_Name
duplicates drop MP_ID, force
save mpnames, replace
restore

gen count = 1
collapse (sum) count , by(prediction MP_ID year)
drop if prediction==1	// Drop procedure topics
bysort MP_ID year: egen totalspeeches = sum(count)
gen fraction = count/totalspeeches

* Change: merge it back
merge m:1 MP_ID using mpnames

* Change: now include prime ministers and the opposition leaders between 1980 and 2020
* PMs
gen is_pm = 0
replace is_pm = 1 if ///
(MP_Name == "justin trudeau") | ///
(MP_Name == "paul edgar philippe martin") | ///
(MP_Name == "pierre trudeau" | MP_Name == "pierre elliott trudeau") | ///
(MP_Name == "stephen harper" & year >= 2006 & year <= 2015) | ///
((MP_Name == "jean chretien" | MP_Name == "joseph jacques jean chretien") & year >= 1993 & year <= 2004) | ///
((MP_Name == "brian mulroney" | MP_Name == "martin brian mulroney") & year >= 1984 & year <= 1993)

* Opposition leaders
gen is_opposition = 0
replace is_opposition = 1 if ///
(MP_Name == "charles joseph clark" | MP_Name == "joe clark" | MP_Name == "joseph clark") | ///
(MP_Name == "john napier turner") | ///
(MP_Name == "lucien bouchard") | ///
(MP_Name == "michel gauthier") | ///
(MP_Name == "stephane dion") | ///
(MP_Name == "michael ignatieff") | ///
(MP_Name == "jack layton") | ///
(MP_Name == "thomas mulcair" | MP_Name == "thomas j. mulcair") | ///
(MP_Name == "rona ambrose") | ///
(MP_Name == "andrew scheer") | ///
(MP_Name == "ernest preston manning" | MP_Name == "preston manning") | ///
(MP_Name == "stockwell day" | MP_Name == "stockwell burt day") | ///
(MP_Name == "stephen harper" & year >= 2002 & year <= 2006) | ///
((MP_Name == "brian mulroney" | MP_Name == "martin brian mulroney") & year >= 1983 & year <= 1984) | ///
((MP_Name == "jean chretien" | MP_Name == "joseph jacques jean chretien") & year >= 1990 & year <= 1993)



label def prediction ///
	0 "People and Governance" ///
	1 "Procedure" ///
	2 "Trade" ///
	3 "Health and Child Care" ///
	4 "Broad Governance Topics" ///
	5 "Gender and Social Issues" ///
	6 "Finance and Taxes" ///
	7 "Employment and Economy" ///
	8 "Crime and Law" ///
	9 "Agriculture"


label val prediction prediction


*Change: plot PM speeches, Opposition party leader speeches, and all other parl. speeches
twoway ///
(lpoly fraction year if is_pm, bw(1) by(prediction) color(red)) ///
(lpoly fraction year if is_opposition, bw(1) by(prediction) color(blue)) ///
(lpoly fraction year if is_pm == 0 & is_opposition == 0, bw(1) by(prediction) color(green)) ///
, legend(order(1 "Prime Minister" 2 "Opposition Leader" 3 "Other Parliamentarians"))


graph export "Final/MP_Speeches.png", replace width(1000)