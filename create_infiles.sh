#!/bin /bash
if [ $# -eq 5 ] #tsekarw ta orismata
  then
	zero=0
	if (( $4>=$zero ))
	then 
		if (( $5>=$zero ))
		then 
				rid=0 
				mkdir $3 #ftiaxnw to input_dir
				cd $3
				while  read  line1 #gia kathe xwra
					do
					mkdir $line1 #ftiaxnw fakelo gia kathe xwra mesa sto input_dir
					cd $line1
					for (( c=1; c<=$4; c++ ))
					do
							#---RANDOM DATE-----
							Day_Range=30
							day=$RANDOM
							one=1
							let day%=$Day_Range
							day=$((day+one))
							Month_Range=12
							month=$RANDOM
							let month%=$Month_Range
							month=$((month+one))
							Year_Range=20
							y2=2000
							year=$RANDOM
							let year%=$Year_Range
							year=$((year+y2))
							ten=10
							if (("$day" <"$ten"))
								then 
									day="0$day" #frontizw na einai typou 05 kai oxi 5
							fi

							if (("$month" < "$ten"))
								then 
									month="0$month"
							fi	
							fulldate="$day-$month-$year"
							touch $fulldate #ftiaxnw to arxeio me onoma tin imerominia ayti
							for (( b=1; b<=$5; b++ ))
							do
									onee=1
									rid=$((rid+onee)) #ftiaxnw monadika ids tis morfis rx opou x einai enas monadikos arithmos(athroisma)
							#-------RANDOM ENTRANCE(ENTER/EXIT)-----		
									ent0="ENTER"
									ent1="EXIT"
									rand=$RANDOM
									two=2
									let rand%=$two
									if (("$rand"=="$one"))	
										then entrance=$ent0
									else
										entrance=$ent1
									fi
							#-------RANDOM FIRST/LAST NAMES-------
									var=10 #frontizw na einai apo 3 ws 12 xaraktires
									finish=$RANDOM
									finish2=$RANDOM
									let finish%=$var
									let finish2%=$var
									var=3
									finish=$((finish+var))
									finish2=$((finish2+var))
									first_name=()
									last_name=()
									for i in {a..z}; do
										first_name[$RANDOM]=$i
									done
									for i in {a..z}; do
										last_name[$RANDOM]=$i
									done
									IFS="" eval 'fn=" ${first_name[*]::finish}"'
									IFS="" eval 'ln="${last_name[*]::finish}"'
						#---------RANDOM DISEASE FROM DISEASE FILE----------
									disease=$(shuf -n 1 $2)
						#---------RANDOM AGE (1-120)-------------------			
									var=120
									age=$RANDOM
									let age%=$var
									var=1
									age=$((age+var))
									echo "r$rid $entrance $fn $ln $disease $age">>$fulldate  #edw ta grafei se mia grammi tou arxeiou
								done
								
						done		


					cd ..	
					done  < $1
				cd ..
				
				exit  0
			else
				echo "negative arguments are not allowed"
			fi

		else
			echo "negative arguments are not allowed"
		fi
else
	echo "No arguments supplied"
fi

