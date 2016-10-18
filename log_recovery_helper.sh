#!/bin/bash

main () {
	if [ -z "$1" -o -z "$2" -o -z "$3" ]; then
		echo "Error: Please provide a filename template for log files in this format: <prefix> <this_node_id> <suffix>"
		exit 1
	fi

	file_prefix=$1
	local_id=$2
	logfile_suffix=$3
	viewfile_suffix="paxosstate"
	metadata_suffix="metadata"
	derecho_wd=$(pwd)

	declare -a view
	view_file=${file_prefix}${local_id}.${logfile_suffix}.${viewfile_suffix}
	mapfile -t view < <(./parse_state_file $view_file) 
	#save my_rank and local_ip before synchronizing views
	my_rank=${view[6]}
	member_ids=(${view[1]})
	local_ip=${member_ips[$my_rank]}

	echo "Synchronizing saved views with other members..."
	until fetch_and_scan_views; do
		: #nothing
	done
	#The function has modified $view to the latest view (all variables are global in bash)
	member_ids=(${view[1]})
	member_ips=(${view[2]})
	num_members=${view[5]}
	if [ -e latest_view.${viewfile_suffix} ]; then
		#Replace the node's local view file with latest view, so Derecho can read it when it starts up
		printf '%s\n' "${view[@]}" | ./create_state_file $view_file 
		echo "$view_file has been updated to the last known view."
	else 
		echo "$view_file already contains the latest view."
	fi
	#Record the ranks of members that responded, so we don't attempt to contact dead ones
	declare -a live_members
	for ((rank=0; rank < num_members; rank++)); do
		if [ ${member_ids[$rank]} -eq $local_id ]; then 
			continue
		fi
		received_file_name=${file_prefix}${member_ids[$rank]}.${logfile_suffix}.${viewfile_suffix}
		if [ -e $receive_dir/$received_file_name ]; then
			live_members+=("$rank")
		fi
	done

	#Message numbers are tuples (vid, sender, index)
	#Start with the local log's latest message number
	local_message_tuple=($(./latest_logged_message ${file_prefix}${local_id}.${logfile_suffix}))
	latest_message_tuple=("${local_message_tuple[@]}")
	longest_log_rank=$my_rank	#rank of the local node
	
	#Ask each live member what its latest message number is
	for rank in ${live_members[@]}; do
		#This only works if we have public-key access to all the other nodes! Otherwise SSH will prompt for username and password, which breaks everything
		msg_num_tuple=($(ssh -q ${member_ips[$rank]} "${derecho_wd}/latest_logged_message ${derecho_wd}/${file_prefix}${member_ids[$rank]}.${logfile_suffix}"))
		if (( $(compare_message_nums ${msg_num_tuple[@]} ${latest_message_tuple[@]}) > 0 )); then
			latest_message_tuple=("${msg_num_tuple[@]}")
			longest_log_rank=$rank
		fi
	done

	if [ ${member_ids[$longest_log_rank]} -eq $local_id ]; then
		echo "This node has the longest log."
		exit 0
	else 
		echo "Longest log is at node with rank $longest_log_rank, which has ID ${member_ids[$longest_log_rank]} and IP address ${member_ips[$longest_log_rank]}"
		echo "Appending its tail to the local log..."
		longest_log_filename="${derecho_wd}/${file_prefix}${member_ids[$longest_log_rank]}.${logfile_suffix}"
		log_tail_bytes=$(ssh -q ${member_ips[$longest_log_rank]} "${derecho_wd}/log_tail_length $longest_log_filename ${local_message_tuple[@]}")
		metadata_tail_bytes=$(ssh -q ${member_ips[$longest_log_rank]} "${derecho_wd}/log_tail_length -m $longest_log_filename ${local_message_tuple[@]}")
		#Append bytes from the end of the remote log to the local log
		nc -ld 6666 >> ${file_prefix}${local_id}.${logfile_suffix} &
		ssh -q ${member_ips[$longest_log_rank]} "tail -c $log_tail_bytes $longest_log_filename | nc $local_ip 6666"  
		#Repeat with the metadata file
		nc -ld 6667 >> ${file_prefix}${local_id}.${logfile_suffix}.${metadata_suffix} &
		ssh -q ${member_ips[$longest_log_rank]} "tail -c $metadata_tail_bytes $longest_log_filename.${metadata_suffix} | nc $local_ip 6667"  
	fi
}


fetch_and_scan_views () {
	vid=${view[0]}

	#Bash will expand the space-delimited list of IPs to an array literal
	member_ips=(${view[2]})
	member_ids=(${view[1]})
	num_members=${view[5]}
	receive_dir=$(mktemp -d)
	#Attempt to fetch the view-file from each member of the current view
	for ((rank=0; rank < num_members; rank++)); do
		if [ ${member_ids[$rank]} -eq $local_id ]; then 
			continue
		fi
		scp -o ConnectTimeout=1 ${member_ips[$rank]}:${derecho_wd}/${file_prefix}${member_ids[$rank]}.${logfile_suffix}.${viewfile_suffix} ${receive_dir} &
	done
	#Wait for scp attempts to either succeed or time out
	wait

	num_responses=0
	quorum_size=$(( num_members/2 + 1 ))
	#Inspect each received view file to see if it has a larger vid
	for ((rank=0; rank < num_members; rank++)); do
		if [ ${member_ids[$rank]} -eq $local_id ]; then 
			continue
		fi
		received_file_name=${file_prefix}${member_ids[$rank]}.${logfile_suffix}.${viewfile_suffix}
		if [ -e ${receive_dir}/$received_file_name ]; then
			(( num_responses++ ))
			declare -a other_view
			mapfile -t other_view < <(./parse_state_file ${receive_dir}/$received_file_name)
			if (( ${other_view[0]} > $vid )); then
				mv ${receive_dir}/$received_file_name ./latest_view.${viewfile_suffix}
				view=("${other_view[@]}") #reassign view to other_view
				rm -rf ${receive_dir} 
				return 1 #this will make the function retry from the beginning with the new view
				#Future work: Use symlinks from IP address -> view file to record which nodes we've already contacted, in case they're shared between the old view and the new view (expected)
			fi
		fi
	done
	#if we got here, none of the views contained a higher vid
	if (( $num_responses < $quorum_size )); then
		echo "Failed to reach a quorum of the last known view. Retrying after 5 seconds..."
		sleep 5
		rm -rf ${receive_dir} 
		return 1
	fi
	#a quorum of members are alive and we have the latest view!
	rm -rf ${receive_dir}
	return 0
}

compare_message_nums() {
	local msg_num_1=("$1" "$2" "$3")
	local msg_num_2=("$4" "$5" "$6")
	#Just swap sender and index, then call lexical_compare_triple
	lexical_compare_triple "${msg_num_1[0]}" "${msg_num_1[2]}" "${msg_num_1[1]}" "${msg_num_2[0]}" "${msg_num_2[2]}" "${msg_num_2[1]}"
}

lexical_compare_triple() {
	local triple1=("$1" "$2" "$3")
	local triple2=("$4" "$5" "$6")
	if (( ${triple1[0]} > ${triple2[0]} )); then
		echo "1"
		return
	fi
	if (( ${triple1[0]} == ${triple2[0]} )); then
		if (( ${triple1[1]} > ${triple2[1]} )); then
			echo "1"
			return
		fi
		if (( ${triple1[1]} == ${triple2[1]} )); then
			if (( ${triple1[2]} > ${triple2[2]} )); then
				echo "1"
				return
			fi
			if (( ${triple1[2]} == ${triple2[2]} )); then
				echo "0"
				return
			fi
		fi
	fi
	echo "-1"
	return 
}

#Execution actually starts here, and passes all parameters to main()
main "$@"
