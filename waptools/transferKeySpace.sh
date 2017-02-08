#!/bin/bash 
#Author : Varun Barala
#Purpose: This script is just an wrapper on the top of original SSTable utility

dataDir='/var/lib/cassandra/data'
timeStampToGenerateDir='752201556'
# this is to initialize with minimum
#timeStamp='752201556'
# Param1:  --tenantId <tenantId>
# Param2:  --replaceWith <replacedTenantId>
# Param3:  --timestamp for which you want to take snapshot 
timeStamp='752201556'

# these are the essential parameters for our utility
tenantId=''
replaceWith=''
nodeAddress=''
keyspaceName=''
targetKeyspace=''
cfNames=''
mode=''


# this is to initialize the data directory 
initDataDir(){
	echo "please enter your data directory, example :-> /var/lib/cassandra/data/keyspace"
	echo "---------------------------------------------------------------------"
	echo ""
	read -p "[input directory]:-" dataDir
	echo ""
	echo ""
}



# mode validation whether user has entered the correct mode or not
modeValidation(){
	#echo "$1 value of mode"
	if [ "$1" == "G" ] || [ "$1" == "F" ] || [ "$1" == "GT" ]; then
		echo "processing............"
	else
		echo "You have entered wrong mode:: " $1
		initMode
	fi
}



# this is to get mode 
initMode(){
	echo "We support four modes===================================="
	echo "mode: F -> to just filter the data for given any tenant"
	echo "mode: G -> to just generate the data for modified tenantId"
	echo "mode: GT -> to generate as well as transfer the data"
	echo "========================================================="
	read -p "Please choose a mode:: " mode
	modeValidation $mode
}


# this will ensure that all cf is having this time snapshot
checkValidity(){
	countTotalDir=`find $dataDir/* -maxdepth 0 -type d | wc -l` 
	countSnapshotDir=`find $dataDir -type d -name $timeStamp | wc -l`
	echo "---------------------------------------------------------------------"
	echo "total CF :: $countTotalDir" " total snapshot dir :: $countSnapshotDir"
	echo "---------------------------------------------------------------------"
	echo ""
	if [ "$countTotalDir" -eq "$countSnapshotDir" ];
	then
		echo "validation was successfull!!!"
		echo ""
		echo ""
	else
		echo ""
		echo "It seems all CF are not having the same snapshot version.. Redirecting back to snapshot step"
		echo ""
		getLatestSnapshot
	fi
}

getParamsForUtility(){
	read -p "enter the tenantId: " tenantId
	read -p "enter replacing tenantId: " replaceWith
	read -p "enter node ip address: [If you multiple then pass them whitespace separated] :: " nodeAddress
}

transferAll(){
	## Loop through all the CF
	getParamsForUtility
	allSnapshotDir=`find $dataDir -type d -name $timeStamp`
	while IFS=' ' read -ra ADDR; 
	do
		for i in "${ADDR[@]}";
		do
			sudo ./waptranspoutility -d $nodeAddress --tenantId $tenantId --replaceWith $replaceWith $i			
		done
	done <<< "$allSnapshotDir"
}

moveLatestDirWithLatestTimeStamp(){
	timeStampToGenerateDir=`date +%s`
	sudo mkdir -p modifiedData/Transferred/data/$timeStampToGenerateDir
	sudo mv modifiedData/Generated/data/$timeStamp modifiedData/Transferred/data/$timeStampToGenerateDir
}


getFilterOnlyParameters(){
	echo "Please enter all relevant parameters for filter only mode:"
	echo "----------------------------------------------------------"
	read -p "please enter the tenantId for which you want to filter the data:: " tenantId
}

getGenerateOnlyParameters(){
	echo "Please enter all relevant parameters for generate only mode:"
	echo "------------------------------------------------------------"
	read -p "please enter the current keyspace name:: " keyspaceName
	read -p "please enter the tenantId:: " tenantId
	read -p "please enter the replacing tenantId:: " replaceWith
	read -p "Please enter the target keyspace:: " targetKeyspace
}


getGenerateAndTransferOnly(){
	echo "Please enter all relevant parameters for generate only mode:"
	echo "------------------------------------------------------------"
	read -p "please enter the current keyspace name:: " keyspaceName
	read -p "please enter the tenantId:: " tenantId
	read -p "please enter the replacing tenantId:: " replaceWith
	read -p "Please enter the target keyspace:: " targetKeyspace
	read -p "please enter all the nodes:: " nodeAddress
}

filterOnly(){
	 ## get the parameters
	 getFilterOnlyParameters
 	 allSnapshotDir=`find $dataDir -type d -name $timeStamp`
         while IFS=' ' read -ra ADDR; 
         do
         	for i in "${ADDR[@]}";
                do
                         sudo ./waptranspoutility -mode $mode -d 172.26.152.172 --tenantId $tenantId -timestamp $timeStamp $i
                done
        done <<< "$allSnapshotDir"		
}


generateOnly(){
	findLatestTimeStamp
 	getGenerateOnlyParameters
	# hard code it here
	#allTargetDir=`ls modifiedData/Filtered/data/"$timeStamp"/$keyspaceName`
	allTargetDir=`find modifiedData/Filtered/data/$timeStamp/$keyspaceName/* -maxdepth 0 -type d`
	while IFS=' ' read -ra ADDR;
	do
		for i in "${ADDR[@]}"
		do
			sudo ./waptranspoutility -k $targetKeyspace -mode $mode -d 172.26.152.172 --tenantId $tenantId --replaceWith $replaceWith -timestamp $timeStamp $i
		done
	done <<< "$allTargetDir"
}

generateAndTransferOnly(){
 # get all nodes
 	findLatestTimeStamp
	getGenerateAndTransferOnly
	allTargetDir=`find modifiedData/Filtered/data/$timeStamp/$keyspaceName/* -maxdepth 0 -type d`
	while IFS=' ' read -ra ADDR;
	do
		for i in "${ADDR[@]}"
		do
			sudo ./waptranspoutility -k $targetKeyspace -mode $mode -d "$nodeAddress" --tenantId $tenantId --replaceWith $replaceWith -timestamp $timeStamp $i
		done
	done <<< "$allTargetDir"
	
	moveLatestDirWithLatestTimeStamp
}


transferFew(){
	## Loop through given CF this function will get one param :-> list of all the CF
	getParamsForUtility
	while IFS=' ' read -ra ADDR;
	do
		for i in "${ADDR[@]}";
		do
			sudo ./waptranspoutility -d "$nodeAddress" --tenantId $tenantId --replaceWith $replaceWith $i
		done
	done <<< "$1"
}



getKeySpaceInfo(){
	#read -p "Transfer whole keyspace [Y/N]:" wholeKS
	#if [ $wholeKS = 'Y' ] || [ $wholeKS = 'y' ]
	#then
	read -p "please enter name of current keyspace: " keyspaceName
		#transferAll $keyspaceName
	#else 
		#read -p "please enter all the CF name , separated| [CF]: " cfNames
		#transferFew $cfNames
	#fi

}

getLatestTimeStamp(){
	# this will give you the latest timestamp if there does exist some snapshot 
	timeStamp=`ls $1 -1 | tail -1`	
	#echo "latest snapshot found : $timeStamp"
	checkValidity $timeStamp
	getKeySpaceInfo

	if [ "$mode" == 'F' ]; then
		filterOnly
	elif [ "$mode" == 'G' ]; then
		generateOnly
	elif [ "$mode" == 'GT' ]; then
		generateAndTransferOnly
	fi
	# now get option for whole keyspace or some tables
}

getLatestSnapshot(){
	# grep snapshot and compare with all results -> inside that directory
	snapshotDir=`find $dataDir -type d -name "snapshots" | head -1`
	if [ -z $snapshotDir ];
	then
		echo "there is no snapshot Dir exist!!"
		echo "We will need to take snapshot !! Redirecting to snapshot step...."
		echo ""
		snapshot
	else
		getLatestTimeStamp $snapshotDir
	fi
	echo "found latest timestamp"
}


findLatestTimeStamp(){
	timeStamp=`ls modifiedData/Filtered/data/ | tail -1` 
#	if [ "$mode" == 'F' ]; then
#                filterOnly
#        elif [ "$mode" == 'G' ]; then
#                generateOnly
#        elif [ "$mode" == 'GT' ]; then
#                generateAndTransferOnly
#        fi
}

# If they have recently taken the snapshot then We will -> get latest time-stamp or 
# else we will take snapshot
snapshot(){
	read -p "Did you take the snapshot [Y/N/SKIP]:" didSnapshot
	echo ""
	if [ $didSnapshot = 'Y' ] || [ $didSnapshot = 'y' ];
	then
		getLatestSnapshot
		# TODO get latest snapshot directory
		# check whether snapshot for entire keyspace has been taken or not
	elif [ $didSnapshot = 'N' ] || [ $didSnapshot = 'n' ]; then
		echo "Going to take snapshot and It will take little time!!"
		../../bin/nodetool snapshot -- $keyspaceName
		getLatestSnapshot		
		# take snapshot and run the above function to get latest keyspace 
		# wait after snapshot command 
	else
		echo "going to skip this step"
		findLatestTimeStamp
	fi
}

runMainUtility(){
	echo "here we will read all related paramter"
}

setCurrentTimeStamp(){
	timeStampToGenerateDir=`date +%s`
	# TODO current plan is to generate corresponding to given snapshot
}

main(){
	initMode
	if [ "$mode" == 'G' ]; then
		generateOnly
	elif [ "$mode" == 'GT' ]; then
		echo "IT was here"
		generateAndTransferOnly
	else 
		initDataDir
		snapshot
	fi	
}

main
