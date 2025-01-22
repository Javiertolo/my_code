update maintenance.task t 
set task_config = 'where #$file_modified_time# > (CURRENT_DATE - INTERVAL "1" day)'
where inventory_id = 
(select id from maintenance.inventory i
where entity = 'offers' )
and t.task_type_id = 3