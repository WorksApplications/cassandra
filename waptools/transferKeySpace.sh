#!/bin/bash
#Author : Varun Barala
#Purpose: This script is just an wrapper on the top of original SSTable utility

dataDir='/var/lib/cassandra/data'

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
cfNames=''

# this is to initialize the data directory 
initDataDir(){
	echo "please enter your data directory, example :-> /var/lib/cassandra/data"
	echo "---------------------------------------------------------------------"
	echo ""
	read -p "[input directory]:-" dataDir
	echo ""
	echo ""
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
			./waptranspoutility -d $nodeAddress --tenantId $tenantId --replaceWith $replaceWith $i			
		done
	done <<< "$allSnapshotDir"
}


transferFew(){
	## Loop through given CF this function will get one param :-> list of all the CF
	getParamsForUtility
	while IFS=' ' read -ra ADDR;
	do
		for i in "${ADDR[@]}";
		do
			./waptranspoutility -d $nodeAddress --tenantId $tenantId --replaceWith $replaceWith $i
		done
	done <<< "$1"
}



getKeySpaceInfo(){
	read -p "Want to  tansfer whole keyspace [Y/N]:" wholeKS
	if [ $wholeKS = 'Y' ] || [ $wholeKS = 'y' ]
	then
		read -p "please enter the keyspace name: " keyspaceName
		transferAll $keyspaceName
	else 
		read -p "please enter all the CF name , separated| [CF]: " cfNames
		transferFew $cfNames
	fi

}

getLatestTimeStamp(){
	# this will give you the latest timestamp if there does exist some snapshot 
	timeStamp=`ls $1 -1 | tail -1`	
	echo "latest wsnapshot found : $timeStamp"
	checkValidity $timeStamp
	getKeySpaceInfo
	# now get option for whole keyspace or some tables
	echo "Ohh it stored as a timestamp but now Q is how to compare the dir name"	
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


# If they have recently taken the snapshot then We will -> get latest time-stamp or 
# else we will take snapshot
snapshot(){
	read -p "Did you take the snapshot [Y/N]:" didSnapshot
	echo ""
	if [ $didSnapshot = 'Y' ] || [ $didSnapshot = 'y' ];
	then
		getLatestSnapshot
		# TODO get latest snapshot directory
		# check whether snapshot for entire keyspace has been taken or not
	else
		echo "I'm going to take snapshot and It will take little time!!"
		../../bin/nodetool snapshot -- $keyspaceName
		getLatestSnapshot		
		# take snapshot and run the above function to get latest keyspace 
		# wait after snapshot command 
	fi
}

runMainUtility(){
	echo "here we will read all related paramter"
}

main(){
	initDataDir
	snapshot	
}

main
