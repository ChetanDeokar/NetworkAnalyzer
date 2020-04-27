from helpers.call_drop_helper import CallDropHelper


call_drop_obj = CallDropHelper('919860465205')
msisdn_df = call_drop_obj.get_msisdn_base_view()
#table_df = call_drop_obj.read_hive_table("movies", 'test_table', where_clause="MSISDN='919860465205'")
#print msisdn_df.rdd.map(tuple).collect()
#print table_df.rdd.map(tuple).collect()
#assert msisdn_df.rdd.map(tuple).collect() == table_df.rdd.map(tuple).collect()
df_with_counts = call_drop_obj.process_for_counts_and_ageing_days(msisdn_df)
print df_with_counts.rdd.map(tuple).collect()
df_with_counts.cache()
total_cell_call_counts = call_drop_obj.process_for_total_cell_calls_count(df_with_counts)
print total_cell_call_counts.rdd.map(tuple).collect()
total_call_count = call_drop_obj.get_total_call_count(df_with_counts)
print total_call_count
joined_dataset = call_drop_obj.inner_join_dataframe_over_column(df_with_counts, total_cell_call_counts, 'CellId')
print joined_dataset.rdd.map(tuple).collect()
dataset_with_probability = call_drop_obj.calculate_call_passing_probability(joined_dataset, total_call_count)
print dataset_with_probability.rdd.map(tuple).collect()
top_cells = call_drop_obj.get_ranked_list_desc_order(dataset_with_probability)
print top_cells.rdd.map(tuple).collect()
call_drop_obj.stop_session()
