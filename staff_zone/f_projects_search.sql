-- DROP FUNCTION sz.f_projects_search(text, _text, _text, varchar, date, date, jsonb);

CREATE OR REPLACE FUNCTION sz.f_projects_search(p_search_by text, p_clients text[], p_branches text[], p_status character varying, p_last_visit_date_start date, p_last_visit_date_end date, p_pagination_context jsonb)
 RETURNS jsonb
 LANGUAGE plpgsql
AS $function$
--------------------------------------------------------------------------------
/*

Name:           f_projects_search

Type:           Function 

Author:         diego.soto@bairesdev.com

Creation Date:  2024-07-11

Description:    Search projects based on given parameters:

Parameters:     -  p_search_by          TEXT         - Search criteria
                -  p_clients            TEXT[]       - Clients filter
                -  p_status             VARCHAR(20)  - Project status filter
                -  p_last_visit_date_start DATE       - Last visit date start filter
                -  p_last_visit_date_end   DATE       - Last visit date end filter
                -  p_pagination_context JSONB        - Pagination object
                                                       {
                                                         "limit":100,
                                                         "offset":,
                                                         "sort_column":"",
                                                         "sort_direction":"ASC",
                                                       }

Changes:        2024-07-11  Sprint_11  diego.soto  Initial version

*/
--------------------------------------------------------------------------------

DECLARE
    v_limit             INT;
    v_offset            INT;
    v_sort_column       TEXT;
    v_sort_direction    TEXT;

    v_response          JSONB;
BEGIN

    -- Assign pagination variables

    v_limit          := COALESCE((p_pagination_context->>'limit')::INTEGER
                               , 100);

    v_offset         := COALESCE((p_pagination_context->>'offset')::INTEGER
                               , 0);

    v_sort_column    := (p_pagination_context->>'sort_column')::TEXT;

    v_sort_direction := COALESCE((p_pagination_context->>'sort_direction')::TEXT
                               , 'ASC');


    -- Query
    WITH cte_raw_data AS (
        SELECT
            p.project_id
          , p.project_code
          , p.project_name
          , c.client_id
          , c.client_name
          , b.branch_id
          , b.branch_code
          , b.branch_name
          , v.visit_date
          , p.status
          -- Control Columns
          , COUNT(*) OVER()     AS total_rows
          , CASE WHEN v_sort_column = 'project_code'::TEXT
                THEN ROW_NUMBER() OVER(ORDER BY p.project_code)
                WHEN v_sort_column = 'project_name'::TEXT
                THEN ROW_NUMBER() OVER(ORDER BY p.project_name)
                WHEN v_sort_column = 'client_name'::TEXT
                THEN ROW_NUMBER() OVER(ORDER BY c.client_name)
                WHEN v_sort_column = 'branch_name'::TEXT
                THEN ROW_NUMBER() OVER(ORDER BY b.branch_name)
                WHEN v_sort_column = 'visit_date'::TEXT
                THEN ROW_NUMBER() OVER(ORDER BY v.visit_date)
                WHEN v_sort_column = 'status'::TEXT
                THEN ROW_NUMBER() OVER(ORDER BY p.status)
                -- Default --> client_id
                ELSE ROW_NUMBER() OVER(ORDER BY p.project_code)
            END                 AS row_order
        FROM
            sz.projects p
        -- @ToDo: Implement Join with real visits
        LEFT JOIN LATERAL(
            SELECT
                v.visit_date
            FROM 
                sz.project_site_visits v
            WHERE
                v.project_id = p.project_id
            ORDER BY
                v.visit_date DESC
            LIMIT 1
        ) v
        ON TRUE --IsLateralJoin

        JOIN sz.clients c
        ON c.client_id = p.client_id
        
        JOIN
            sz.branches b
        ON b.branch_id = p.branch_id

        WHERE
            p.is_row_active IS TRUE

        -- Project status filter
        AND (p_status   IS NULL
            OR p.status = p_status)

        -- last_visit_date_start
        AND (p_last_visit_date_start IS NULL
            OR v.visit_date >= p_last_visit_date_start)
        -- last_visit_date_end
        AND (p_last_visit_date_end IS NULL
            OR v.visit_date <= p_last_visit_date_end) 

        -- Clients Filter
        AND (p_clients IS NULL
            OR c.client_id::TEXT = ANY(p_clients))

        -- Branches Filter
        AND (p_branches IS NULL
            OR b.branch_id::TEXT = ANY(p_branches))

        -- Search by
        AND (p_search_by IS NULL
            OR p.project_code
               || ' ' || p.project_name  ILIKE '%'|| p_search_by ||'%')
    )
    SELECT 
        JSONB_AGG(
            ROW_TO_JSON(x)
        ) AS comp_code
    INTO
        v_response
    FROM (
        SELECT
          x.project_id
        , x.project_code
        , x.project_name
        , x.client_id
        , x.client_name
        , x.branch_id
        , x.branch_code
        , x.branch_name
        , x.visit_date
        , x.status
        , x.total_rows
        FROM
            cte_raw_data x
        ORDER BY
            -- Case For ASC Sort
            CASE WHEN v_sort_direction = 'ASC'::TEXT
                THEN x.row_order
                ELSE x.total_rows
            END ASC
            -- Case For DESC Sort
        , CASE WHEN v_sort_direction = 'DESC'::TEXT
                THEN x.row_order
                ELSE x.total_rows
            END DESC
        LIMIT
            v_limit
        OFFSET
            v_offset
    ) AS x;

    -- Return the response
    RETURN 
        v_response;

END;
$function$
;
