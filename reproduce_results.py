from nemlite import input_generator, engine
import pandas as pd

# Note running this file as is will consume large amounts of disk space and take several weeks, probably try running it
# for a much smaller time window first.

# Specify some locations to save data. Create and specify your own!
raw_data = 'your folder path to cache raw data from AEMO'
filtered_data = 'your folder path to save filtered AEMO data for running nemlite'

# Specify the backcast period. Choose a short period, it will probably not work for some time back in history when the
# AEMO data was structured differently.
start_times = ['2011/01/01 00:00:00', '2011/02/01 00:00:00', '2011/03/01 00:00:00', '2011/04/01 00:00:00',
               '2011/05/01 00:00:00', '2011/06/01 00:00:00', '2011/07/01 00:00:00', '2011/08/01 00:00:00',
               '2011/09/01 00:00:00', '2011/10/01 00:00:00', '2011/11/01 00:00:00', '2011/12/01 00:00:00',
               '2017/01/01 00:00:00', '2017/02/01 00:00:00', '2017/03/01 00:00:00', '2017/04/01 00:00:00',
               '2017/05/01 00:00:00', '2017/06/01 00:00:00', '2017/07/01 00:00:00', '2017/08/01 00:00:00',
               '2017/09/01 00:00:00', '2017/10/01 00:00:00', '2017/11/01 00:00:00', '2017/12/01 00:00:00']

end_times = ['2011/02/01 00:00:00', '2011/03/01 00:00:00', '2011/04/01 00:00:00', '2011/05/01 00:00:00',
             '2011/06/01 00:00:00', '2011/07/01 00:00:00', '2011/08/01 00:00:00', '2011/09/01 00:00:00',
             '2011/10/01 00:00:00', '2011/11/01 00:00:00', '2011/12/01 00:00:00', '2012/01/01 00:00:00',
             '2017/02/01 00:00:00', '2017/03/01 00:00:00', '2017/04/01 00:00:00', '2017/05/01 00:00:00',
             '2017/06/01 00:00:00', '2017/07/01 00:00:00', '2017/08/01 00:00:00', '2017/09/01 00:00:00',
             '2017/10/01 00:00:00', '2017/11/01 00:00:00', '2017/12/01 00:00:00', '2018/01/01 00:00:00']

for start_time, end_time in zip(start_times, end_times):
    # Create an generator of actual historical NEMDE inputs.
    inputs = input_generator.actual_inputs_replicator(start_time, end_time, raw_data, filtered_data, True)

    # Create a data frame to save the results
    nemlite_results_cumulative = pd.DataFrame()

    # Iterate other the inputs to
    for [gen_info_raw, capacity_bids_raw, initial_conditions, inter_direct_raw, region_req_raw, price_bids_raw,
         inter_seg_definitions, con_point_constraints, inter_gen_constraints, gen_con_data, region_constraints,
         timestamp, inter_demand_coefficients, mnsp_inter, mnsp_price_bids, mnsp_capacity_bids,
         market_cap_and_floor] in inputs:

        nemlite_results, dispatches, inter_flows = engine.run(gen_info_raw, capacity_bids_raw, initial_conditions,
                                                              inter_direct_raw, region_req_raw, price_bids_raw,
                                                              inter_seg_definitions, con_point_constraints,
                                                              inter_gen_constraints, gen_con_data,
                                                              region_constraints, inter_demand_coefficients,
                                                              mnsp_inter, mnsp_price_bids, mnsp_capacity_bids,
                                                              market_cap_and_floor)

        nemlite_results['DateTime'] = timestamp
        nemlite_results_cumulative = pd.concat([nemlite_results_cumulative, nemlite_results])
        print(timestamp)

    nemlite_results_cumulative.to_csv('your_path/price_results_{}_{}.csv'.format(start_time[:4], start_time[5:7]))