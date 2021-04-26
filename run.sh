for non_pk in 0.01 0.05 0.1 0.2; do		
	for num_workers in 1 2 4 6; do
		for filter_magnitude in 1 2 4 8 16 32; do 
			echo $non_pk,$filter_magnitude,$num_workers
			target/release/nonpk_arrow $non_pk $filter_magnitude $num_workers
		done
	done
done

