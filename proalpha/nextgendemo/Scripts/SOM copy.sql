WITH
    --SOM Parameter values
    PARAMS AS (
        SELECT
            34.5 AS p_som_order_process_costs, -- customer-specific  20  34.5
            5 AS p_minimum_order_interval, -- customer-specific
            0.1 AS p_annual_return, -- customer-specific
            5000 AS p_max_date_delta, -- max days for date functions
            9 AS p_part_type_min,
            20 AS p_part_type_max,
            'part_consumption' AS p_som_metric,
            'part_i_d' AS p_som_group_by_column,
            'company' AS p_som_categorical_column,
            'week' AS p_som_frequency, -- week ( *1 day )
            3 AS p_som_period_rpltime_factor, -- factor to replenishment time cuments to get back for periodic calculation
            93 AS p_som_days_to_predict,
            365 AS p_som_days_to_go_back,
            40 AS p_som_historic_data_filled, -- min count of historic values for using prdicted values for som input
            3 AS p_som_multiply_stdev, -- multiplyer for standard deviation to calculate safety margin
            6 AS p_som_prediction_period, -- count p_som_frequency the predicted value for part consumption som input !!! MAX 12 weeks, 3 months, 93 days !!!
            0.10 AS p_som_r2_min -- min R2 value for using prdicted values for som input
        FROM
            DUMMY
    ),
    PAX_DATA AS (
        SELECT
            PA.company,
            PA.part_i_d,
            PA.mvmt_m_r_p_area,
            PA.part_desc1,
            PA.part_group,
            PA.part_group_desc,
            PA.part_type,
            PA.part_type_description,
            PA.export_date_to,
            PA.mvmt_object_i_d,
            PA.mvmt_m_r_p_group,
            PA.pur_order_doc_date,
            PA.pur_order_doc_open,
            PA.pur_order_line_creation_date,
            PA.pur_order_line_requested_date,
            PA.pur_stock_rec_line_creation_date,
            PA.delivery_costs,
            PA.prod_order_start_date,
            PA.prod_order_end_date,
            PA.part_replenishment_time,
            PA.mvmt_creation_date_time,
            PA.mvmt_average_costs_total_corp_cur,
            PA.mvmt_on_hand,
            PA.mvmt_usage,
            PA.mvmt_m_l_m_safety_stock,
            PA.DIO_Days_Weighted,
            COALESCE(MD.part_supp_order_multiple, 1) AS part_supp_order_multiple,
            COALESCE(MD.part_supp_min_order_qty, 1)  AS part_supp_min_order_qty
        FROM
            (
                SELECT
                    company,
                    part_i_d,
                    mvmt_m_r_p_area,
                    part_desc1,
                    part_group,
                    part_group_desc,
                    part_type,
                    part_type_description,
                    export_date_to,
                    mvmt_object_i_d,
                    mvmt_m_r_p_group,
                    pur_order_doc_date,
                    pur_order_doc_open,
                    pur_order_line_creation_date,
                    pur_order_line_requested_date,
                    pur_stock_rec_line_creation_date,
                    prod_order_start_date,
                    prod_order_end_date,
                    part_replenishment_time,
                    mvmt_creation_date_time,
                    mvmt_average_costs_total_corp_cur,
                    mvmt_on_hand,
                    mvmt_usage,
                    mvmt_m_l_m_safety_stock,
                    mvmt_average_costs_del_costs_fix + mvmt_average_costs_del_costs_var        AS delivery_costs,
                    (mvmt_on_hand / NULLIF(mvmt_usage, 0)) * mvmt_average_costs_total_corp_cur AS DIO_Days_Weighted
                FROM
                    EMZ."pa_export"
                WHERE
                    part_i_d IS NOT NULL
            ) PA
            LEFT JOIN (
                SELECT
                    company,
                    part_i_d,
                    MAX(part_supp_order_multiple) AS part_supp_order_multiple,
                    MAX(part_supp_min_order_qty)  AS part_supp_min_order_qty
                FROM
                    EMZ."nemo_master_data"
                WHERE
                    part_i_d IS NOT NULL
                    AND part_supp_o_i_d IS NOT NULL
                    -- AND part_supp_main = TRUE           -- Get values from main part supplier only
                GROUP BY
                    company,
                    part_i_d
            ) MD ON PA.company = MD.company
            AND PA.part_i_d = MD.part_i_d
    ),
    --Batch forecast data rounded values
    FORECAST AS (
        SELECT
            F.categorical_value                                                                                AS company,
            F.group_by_value                                                                                   AS part_i_d,
            MAX(F.prediction_num)                                                                              AS prediction_no,
            COUNT(F.prediction_value_raw)                                                                      AS CNT_prediction_value,
            SUM(F.prediction_value_raw)                                                                        AS prediction_value,
            MAX(F.prediction_value)                                                                            AS agg_prediction_value,
            MAX(F.prediction_upper)                                                                            AS prediction_upper,
            MAX(F.prediction_lower)                                                                            AS prediction_lower,
            MAX(F.historic_data_filled)                                                                        AS historic_data_filled,
            MAX(ABS(V.R2_score))                                                                               AS R2_SCORE,
            MAX(P.p_som_r2_min)                                                                                AS p_r2_min,
            STDDEV (F.prediction_value_raw)                                                                    AS prediction_value_stddev,
            p_som_multiply_stdev * STDDEV (F.prediction_value_raw) AS prediction_value_stddev_multi, -- STDDEV weekly
            SUM(F.prediction_value_raw) / p_som_prediction_period * 4 AS pred_consumption_per_month,
            ROUND(
                SUM(F.prediction_value_raw) / p_som_prediction_period * 4 + (
                    p_som_multiply_stdev * STDDEV (F.prediction_value_raw)
                ),
                3
            ) AS pred_consumption_per_month_secure 
        FROM
            EMZ.PA_BATCH_FORECAST F
            JOIN PARAMS P ON F.metric = p_som_metric
            AND F.frequency = p_som_frequency
            AND F.historic_data_filled >= p_som_historic_data_filled
            AND F.categorical_column = p_som_categorical_column
            AND F.group_by_column = p_som_group_by_column
            AND F.prediction_num < p_som_prediction_period
            LEFT JOIN EMZ.PA_BATCH_FORECAST_VALIDATION V ON V.metric = p_som_metric
            AND V.frequency = p_som_frequency
            AND V.historic_data_filled >= p_som_historic_data_filled
            AND V.categorical_column = p_som_categorical_column
            AND F.categorical_value = V.categorical_value
            AND F.group_by_value = V.group_by_value
            AND V.prediction_date = F.prediction_date
        GROUP BY
            F.categorical_value,
            F.group_by_value,
            p_som_multiply_stdev,
            p_som_prediction_period
        HAVING
            MAX(ABS(V.R2_score)) >= MAX(P.p_som_r2_min)
    ),
    DEFAULT_MRP_AREA AS (
        SELECT
            company,
            part_i_d,
            FIRST_VALUE (
                mvmt_m_r_p_area
                ORDER BY
                    CNT_AREA DESC
            ) AS DEFAULT_MVMT_MRP_AREA
        FROM
            (
                SELECT
                    company,
                    part_i_d,
                    mvmt_m_r_p_area,
                    COUNT(mvmt_m_r_p_area) AS CNT_AREA
                FROM
                    PAX_DATA
                GROUP BY
                    company,
                    part_i_d,
                    mvmt_m_r_p_area
            )
        GROUP BY
            company,
            part_i_d
    ),
    REPL_TIME_CALC_BASE AS ( -- Default MRP-area set on every part document
        SELECT
            P.company,
            P.part_i_d,
            COALESCE(P.mvmt_m_r_p_area, M.DEFAULT_MVMT_MRP_AREA) AS mvmt_m_r_p_area,
            export_date_to,
            part_replenishment_time,
            part_type,
            pur_order_line_creation_date,
            pur_stock_rec_line_creation_date,
            prod_order_start_date,
            prod_order_end_date,
            CASE
                WHEN part_type > 9
                AND part_type < 20 THEN DAYS_BETWEEN (
                    pur_order_line_creation_date,
                    pur_stock_rec_line_creation_date
                )
                WHEN part_type >= 0
                AND part_type <= 9 THEN DAYS_BETWEEN (prod_order_start_date, prod_order_end_date)
            END + 1 AS replenishment_time_diff -- Plus 1 for start and end at the same date
        FROM
            PAX_DATA P
            LEFT JOIN DEFAULT_MRP_AREA M ON P.company = M.company
            AND P.part_i_d = M.part_i_d
        WHERE
            P.part_i_d IS NOT NULL
            AND (
                (
                    pur_order_line_creation_date IS NOT NULL
                    AND pur_stock_rec_line_creation_date IS NOT NULL
                )
                OR (
                    prod_order_start_date IS NOT NULL
                    AND prod_order_end_date IS NOT NULL
                )
            )
    ),
    REPL_TIME_CALC AS (
        SELECT
            company,
            part_i_d,
            mvmt_m_r_p_area,
            MIN(export_date_to)                           AS export_date_to,
            ROUND(AVG(part_replenishment_time), 0)        AS replenishment_time,
            ROUND(AVG(replenishment_time_diff), 0)        AS average_general_replenishment_time,
            ROUND(STDDEV (replenishment_time_diff), 2)    AS Stddev_repl_time
        FROM
            REPL_TIME_CALC_BASE
        GROUP BY
            company,
            part_i_d,
            mvmt_m_r_p_area,
            part_type
    ),
    dio_days AS (
        SELECT
            D.company,
            D.part_i_d,
            D.mvmt_m_r_p_area,
            D.Part_Group,
            D.Part_Type,
            D.part_desc1,
            D.mvmt_m_r_p_group,
            MONTHS_BETWEEN     (
                MIN(D.mvmt_creation_date_time) OVER (
                    PARTITION BY
                        D.part_i_d
                ),
                MAX(D.mvmt_creation_date_time) OVER (
                    PARTITION BY
                        D.part_i_d
                )
            ) AS Number_of_Months,
            LAST_VALUE (
                D.pur_order_doc_open
                ORDER BY
                    D.pur_order_doc_date
            ) AS pur_order_doc_open,
            LAST_VALUE (
                D.pur_order_doc_date
                ORDER BY
                    D.pur_order_doc_date
            ) AS Last_Order_Date,
            R.average_general_replenishment_time AS Avg_General_Replenishment_Time,
            R.Stddev_repl_time,
            D.mvmt_object_i_d,
            LAST_VALUE (
                D.mvmt_average_costs_total_corp_cur
                ORDER BY
                    D.mvmt_object_i_d
            ) * LAST_VALUE (
                D.mvmt_on_hand
                ORDER BY
                    D.mvmt_object_i_d
            ) AS Last_Stock_Value,
            LAST_VALUE (
                D.mvmt_average_costs_total_corp_cur
                ORDER BY
                    D.mvmt_object_i_d
            ) AS Unit_Price, 
            LAST_VALUE (
                D.delivery_costs
                ORDER BY
                    D.mvmt_object_i_d
            ) AS Last_Delivery_Costs, 
            MAX(D.mvmt_usage) AS mvmt_usage,
            FIRST_VALUE (
                D.mvmt_on_hand
                ORDER BY
                    D.mvmt_object_i_d
            ) AS First_Stock_Amount,
            LAST_VALUE (
                D.mvmt_on_hand
                ORDER BY
                    D.mvmt_object_i_d
            ) AS Last_Stock_Amount,
            LAST_VALUE (
                D.mvmt_m_l_m_safety_stock
                ORDER BY
                    D.mvmt_object_i_d
            ) AS Last_Safety_Stock,
            SUM(D.mvmt_usage) AS Part_Consumption,
            CASE
                WHEN D.Part_Type IN (
                    '4',
                    '6',
                    '7',
                    '9',
                    '10',
                    '11',
                    '12',
                    '13',
                    '14',
                    '15',
                    '16',
                    '17',
                    '18',
                    '19'
                ) THEN (
                    CAST(
                        (
                            SUM(D.DIO_Days_Weighted) / NULLIF(SUM(D.mvmt_average_costs_total_corp_cur), 0)
                        ) AS DECIMAL(38, 2)
                    )
                )
            END AS Days_Inventory_Outstanding,
            CASE
                WHEN D.Part_Type IN (
                    '4',
                    '6',
                    '7',
                    '9',
                    '10',
                    '11',
                    '12',
                    '13',
                    '14',
                    '15',
                    '16',
                    '17',
                    '18',
                    '19'
                ) THEN (
                    CAST(
                        SUM(D.mvmt_average_costs_total_corp_cur) AS DECIMAL(38, 2)
                    )
                )
            END AS Cash_Inventory_Outstanding,
            MAX(D.part_supp_order_multiple) AS part_supp_order_multiple,
            MAX(D.part_supp_min_order_qty) AS part_supp_min_order_qty
        FROM
            PAX_DATA D
            LEFT JOIN REPL_TIME_CALC R ON R.company = D.company
            AND R.part_i_d = D.part_i_d
            AND R.mvmt_m_r_p_area = D.mvmt_m_r_p_area
        WHERE
            D.mvmt_m_r_p_group IS NOT NULL
        GROUP BY
            D.company,
            D.part_i_d,
            D.mvmt_m_r_p_area,
            D.Part_Group,
            D.Part_Type,
            D.part_desc1,
            D.mvmt_m_r_p_group,
            D.mvmt_object_i_d,
            D.mvmt_creation_date_time,
            R.average_general_replenishment_time,
            R.Stddev_repl_time
        HAVING
            SUM(D.mvmt_average_costs_total_corp_cur) > 0
    ),
    part_order_periods AS (
        -- Per part the last p_som_period_rpltime_factor x replenshment time ( calculated bevore given ) as start date
        SELECT
            company,
            part_i_d,
            mvmt_m_r_p_area,
            MIN(
                ADD_DAYS (export_date_to, factored_replenishment_time)
            ) AS min_period_date
        FROM
            (
                SELECT
                    company,
                    part_i_d,
                    mvmt_m_r_p_area,
                    export_date_to,
                    p_som_days_to_go_back,
                    CASE
                        WHEN (
                            COALESCE(
                                average_general_replenishment_time,
                                replenishment_time
                            ) * p_som_period_rpltime_factor
                        ) < (p_som_days_to_go_back) THEN COALESCE(
                            average_general_replenishment_time,
                            replenishment_time
                        ) * p_som_period_rpltime_factor * -1
                        ELSE p_som_days_to_go_back * -1
                    END AS factored_replenishment_time
                FROM
                    REPL_TIME_CALC
                    CROSS JOIN PARAMS
            )
        GROUP BY
            company,
            part_i_d,
            mvmt_m_r_p_area
    ),
    part_values_periodic_list AS (
        -- Per part the movements since min_period_date
        SELECT
            X.company,
            X.part_i_d,
            X.mvmt_m_r_p_area,
            OP.min_period_date,
            X.mvmt_creation_date_time,
            X.mvmt_usage,
            X.mvmt_on_hand
        FROM
            PAX_DATA X
            LEFT JOIN part_order_periods OP ON OP.company = X.company
            AND OP.part_i_d = X.part_i_d
        WHERE
            X.mvmt_usage IS NOT NULL
            AND OP.min_period_date <= X.mvmt_creation_date_time
    ),
    part_values_periodic AS (
        -- Per part the consumption since min_period_date
        SELECT
            company,
            part_i_d,
            mvmt_m_r_p_area,
            MIN(min_period_date)        AS min_period_date,
            SUM(mvmt_usage)             AS consumption_periodic,
            COUNT(mvmt_usage)           AS number_of_movements_postings_periodic,
            ROUND(AVG(mvmt_on_hand), 0) AS avg_stock_periodic
        FROM
            part_values_periodic_list
        GROUP BY
            company,
            part_i_d,
            mvmt_m_r_p_area
    ),
    --
    --
    dio_analyzer AS (
        SELECT
            D.company,
            D.part_i_d,
            D.mvmt_m_r_p_area,
            D.Part_Group,
            D.Part_Type,
            D.part_desc1,
            D.mvmt_m_r_p_group,
            LAST_VALUE         (
                D.pur_order_doc_open
                ORDER BY
                    D.Last_Order_Date
            ) AS pur_order_doc_open,
            LAST_VALUE (
                D.Last_Order_Date
                ORDER BY
                    D.Last_Order_Date
            ) AS Last_Order_Date,
            AVG(D.Avg_General_Replenishment_Time) AS Avg_General_Replenishment_Time,
            AVG(D.Stddev_repl_time) AS Stddev_repl_time,
            LAST_VALUE (
                D.Last_Stock_Value
                ORDER BY
                    D.mvmt_object_i_d
            ) AS Last_Stock_Value,
            LAST_VALUE (
                D.Last_Safety_Stock
                ORDER BY
                    D.mvmt_object_i_d
            ) AS Last_Safety_Stock,
            LAST_VALUE (
                D.last_stock_amount
                ORDER BY
                    D.mvmt_object_i_d
            ) AS Last_Stock_Amount,
            LAST_VALUE (
                D.Unit_Price
                ORDER BY
                    D.mvmt_object_i_d
            ) AS Last_Unit_Price, 
            LAST_VALUE (
                D.Last_Delivery_Costs
                ORDER BY
                    D.mvmt_object_i_d
            ) AS Last_Delivery_Costs, 
            (
                SUM(D.part_consumption) / NULLIF(AVG(D.Number_of_Months), 0)
            ) AS Consumption_per_Month, -- Calculate full period !
            COUNT(D.mvmt_usage) AS Number_of_consumption_postings,
            MIN(C.min_period_date) AS min_period_date,
            MIN(C.consumption_periodic) AS consumption_periodic,
            MIN(C.number_of_movements_postings_periodic) AS number_of_movements_postings_periodic,
            MIN(C.avg_stock_periodic) AS avg_stock_periodic,
            MAX(D.part_supp_order_multiple) AS part_supp_order_multiple,
            MAX(D.part_supp_min_order_qty) AS part_supp_min_order_qty
        FROM
            dio_days D
            LEFT JOIN part_values_periodic C ON D.company = C.company
            AND D.part_i_d = C.part_i_d
            AND D.mvmt_m_r_p_area = C.mvmt_m_r_p_area
        GROUP BY
            D.company,
            D.part_i_d,
            D.mvmt_m_r_p_area,
            D.Part_Group,
            D.Part_Type,
            D.part_desc1,
            D.mvmt_m_r_p_group
    ),
    stock_optimization AS (
        SELECT
            D.company,
            D.part_i_d,
            D.mvmt_m_r_p_area,
            D.Part_Group,
            D.Part_Type,
            D.part_desc1,
            D.mvmt_m_r_p_group,
            D.pur_order_doc_open,
            D.Last_Order_Date,
            D.Last_Unit_Price,   
            CASE
                WHEN D.Last_Delivery_Costs IS NOT NULL THEN ROUND(D.Last_Delivery_Costs, 2)
                ELSE AVG(D.Last_Delivery_Costs) OVER ()
            END AS Delivery_Costs, 
            ROUND(D.Avg_General_Replenishment_Time, 0) AS Avg_General_Replenishment_Time,
            ROUND(D.Stddev_repl_time, 2) AS Stddev_replenishment_time,
            ROUND(D.Last_Stock_Value, 2) AS Last_Stock_Value,
            ROUND(D.Last_Stock_Amount, 1) AS Last_Stock_Amount,
            ROUND(D.Last_Safety_Stock, 2) AS Last_Safety_Stock,
            D.Number_of_consumption_postings,
            ROUND(
                COALESCE(
                    F.pred_consumption_per_month_secure,
                    D.Consumption_per_Month
                ),
                1
            ) AS Consumption_per_Month, --Decide historic or predicted values   
            CASE
                WHEN F.R2_SCORE IS NOT NULL THEN 'Zukunft'
                ELSE 'Vergangenheit'   ---„History / Future“ to „Zukunft / Vergangenheit“
            END AS SOM_BASE_INDICATOR,
            D.min_period_date,
            D.consumption_periodic,
            D.number_of_movements_postings_periodic,
            D.avg_stock_periodic,
            P.p_som_order_process_costs AS Order_Process_Costs,
            P.p_minimum_order_interval AS Min_Order_Interval,
            P.p_annual_return,
            D.part_supp_order_multiple,
            D.part_supp_min_order_qty
        FROM
            dio_analyzer D
            CROSS JOIN PARAMS P
            LEFT JOIN FORECAST F ON D.company = F.COMPANY
            AND D.part_i_d = F.part_i_d
    ),
    stock_optimization_calc AS (
        SELECT
            company,
            part_i_d,
            mvmt_m_r_p_area,
            Part_Group,
            Part_Type,
            part_desc1,
            mvmt_m_r_p_group,
            pur_order_doc_open,
            Last_Order_Date,
            Avg_General_Replenishment_Time,
            Last_Stock_Value,
            Last_Stock_Amount,
            Last_Safety_Stock,
            Consumption_per_Month,         
            min_period_date,
            consumption_periodic,
            number_of_movements_postings_periodic,
            avg_stock_periodic,
            Order_Process_Costs, 
            Min_Order_Interval,
            Stddev_replenishment_time,
            Delivery_Costs, 
            Last_Unit_Price, 
            CASE
                WHEN (
                    (Order_Process_Costs + Delivery_Costs) * Consumption_per_Month * 365
                ) / NULLIF((30 * p_annual_return * Last_Unit_Price), 0) > 0 THEN ROUND(
                    SQRT(
                        (
                            (Order_Process_Costs + Delivery_Costs) * Consumption_per_Month * 365
                        ) / NULLIF((30 * p_annual_return * Last_Unit_Price), 0)
                    ),
                    0
                )
            END AS Opt_order_qty_som, 
            SOM_BASE_INDICATOR,
            part_supp_order_multiple,
            part_supp_min_order_qty
        FROM
            stock_optimization
    ),
    stock_optimization_model AS (
        SELECT
            company,
            part_i_d,
            mvmt_m_r_p_area,
            Part_Group,
            Part_Type,
            part_desc1,
            mvmt_m_r_p_group,
            pur_order_doc_open,
            Last_Order_Date,
            Avg_General_Replenishment_Time,
            Last_Stock_Value,
            Last_Stock_Amount,
            Last_Safety_Stock,
            Consumption_per_Month,
            min_period_date,
            consumption_periodic,
            number_of_movements_postings_periodic,
            avg_stock_periodic,
            Order_Process_Costs,
            Min_Order_Interval,
            Stddev_replenishment_time,
            Delivery_Costs,
            Last_Unit_Price,
            part_supp_order_multiple,
            part_supp_min_order_qty,
            Opt_order_qty_som,
            SOM_BASE_INDICATOR,
            CASE
                WHEN Opt_order_qty_som <= part_supp_min_order_qty THEN part_supp_min_order_qty
                WHEN Opt_order_qty_som > part_supp_min_order_qty
                AND Opt_order_qty_som > TO_DECIMAL (part_supp_order_multiple) * (
                    CAST(
                        Opt_order_qty_som / NULLIF(part_supp_order_multiple, 0) AS INTEGER
                    )
                ) THEN TO_DECIMAL (part_supp_order_multiple) * (
                    CAST(
                        Opt_order_qty_som / NULLIF(part_supp_order_multiple, 0) AS INTEGER
                    ) + 1
                )
                WHEN Opt_order_qty_som > part_supp_min_order_qty
                AND Opt_order_qty_som <= TO_DECIMAL (part_supp_order_multiple) * (
                    CAST(
                        Opt_order_qty_som / NULLIF(part_supp_order_multiple, 0) AS INTEGER
                    )
                ) THEN TO_DECIMAL (part_supp_order_multiple) * (
                    CAST(
                        Opt_order_qty_som / NULLIF(part_supp_order_multiple, 0) AS INTEGER
                    )
                )
            END AS Opt_order_qty_supp_min_order_qty,
            CASE
                WHEN Opt_order_qty_som <= part_supp_order_multiple THEN part_supp_order_multiple
                WHEN Opt_order_qty_som > part_supp_order_multiple THEN TO_DECIMAL (part_supp_order_multiple) * (
                    FLOOR(
                        Opt_order_qty_som / NULLIF(part_supp_order_multiple, 0)
                    ) + 1
                )
            END AS Opt_order_qty_supp_order_multiple 
        FROM
            stock_optimization_calc
    ),
    stock_opt_data_best_menge AS (
        SELECT
            company,
            part_i_d,
            mvmt_m_r_p_area,
            Part_Group,
            Part_Type,
            part_desc1,
            mvmt_m_r_p_group,
            Last_Order_Date,
            Avg_General_Replenishment_Time,
            Last_Stock_Value,
            ROUND(Last_Stock_Amount, 0)           AS Last_Stock_Amount,
            Last_Safety_Stock,
            Consumption_per_Month,
            SOM_BASE_INDICATOR,
            min_period_date,
            consumption_periodic,
            number_of_movements_postings_periodic,
            avg_stock_periodic,
            Delivery_Costs,
            Order_Process_Costs,
            Min_Order_Interval,
            Stddev_replenishment_time,
            Last_Unit_Price,
            part_supp_order_multiple,
            part_supp_min_order_qty,
            CASE
                WHEN Min_Order_Interval < ROUND(
                    (
                        Opt_order_qty_supp_min_order_qty / NULLIF((Consumption_per_Month / 30), 0)
                    ),
                    0
                ) THEN ROUND(
                    (
                        Opt_order_qty_supp_min_order_qty / NULLIF((Consumption_per_Month / 30), 0)
                    ),
                    0
                )
                ELSE Min_Order_Interval
            END AS Order_interval,
            Opt_order_qty_supp_min_order_qty AS Opt_order_qty,
            ROUND(
                (
                    Opt_order_qty_supp_min_order_qty + (Consumption_per_Month / 30) * (3 * Stddev_replenishment_time) + Last_Safety_Stock
                ),
                0
            ) AS Recommended_min_stock_value,
            ROUND(
                (
                    (
                        Last_Stock_Amount - (
                            Opt_order_qty_supp_min_order_qty + (Consumption_per_Month / 30) * (3 * Stddev_replenishment_time) + Last_Safety_Stock
                        )
                    ) * (
                        Last_Unit_Price + (Order_Process_Costs + Delivery_Costs) / NULLIF(Opt_order_qty_supp_min_order_qty, 0)
                    )
                ),
                2
            ) AS Optimization_value,
            ROUND(
                (
                    (
                        avg_stock_periodic - (
                            Opt_order_qty_supp_order_multiple + (consumption_periodic / 30) * (3 * Stddev_replenishment_time) + Last_Safety_Stock
                        )
                    ) * (
                        Last_Unit_Price + (Order_Process_Costs + Delivery_Costs) / NULLIF(Opt_order_qty_supp_order_multiple, 0)
                    )
                ),
                2
            ) AS Periodic_Optimization_Value,
            ROUND(
                (
                    CASE
                        WHEN Avg_General_Replenishment_Time <= (
                            CASE
                                WHEN Min_Order_Interval < ROUND(
                                    (
                                        Opt_order_qty_supp_min_order_qty / NULLIF((Consumption_per_Month / 30), 0)
                                    ),
                                    0
                                ) THEN ROUND(
                                    (
                                        Opt_order_qty_supp_min_order_qty / NULLIF((Consumption_per_Month / 30), 0)
                                    ),
                                    0
                                )
                                ELSE Min_Order_Interval
                            END
                        ) THEN (Consumption_per_Month / 30) * (
                            Avg_General_Replenishment_Time + 3 * Stddev_replenishment_time
                        ) + Last_Safety_Stock - CAST(
                            (
                                Avg_General_Replenishment_Time / NULLIF(
                                    (
                                        CASE
                                            WHEN Min_Order_Interval < ROUND(
                                                (
                                                    Opt_order_qty_supp_min_order_qty / NULLIF((Consumption_per_Month / 30), 0)
                                                ),
                                                0
                                            ) THEN ROUND(
                                                (
                                                    Opt_order_qty_supp_min_order_qty / NULLIF((Consumption_per_Month / 30), 0)
                                                ),
                                                0
                                            )
                                            ELSE Min_Order_Interval
                                        END
                                    ),
                                    0
                                )
                            ) AS INTEGER
                        ) * (Consumption_per_Month / 30) * (
                            CASE
                                WHEN Min_Order_Interval < ROUND(
                                    (
                                        Opt_order_qty_supp_min_order_qty / NULLIF((Consumption_per_Month / 30), 0)
                                    ),
                                    0
                                ) THEN ROUND(
                                    (
                                        Opt_order_qty_supp_min_order_qty / NULLIF((Consumption_per_Month / 30), 0)
                                    ),
                                    0
                                )
                                ELSE Min_Order_Interval
                            END
                        )
                        WHEN Avg_General_Replenishment_Time > (
                            CASE
                                WHEN Min_Order_Interval < ROUND(
                                    (
                                        Opt_order_qty_supp_min_order_qty / NULLIF((Consumption_per_Month / 30), 0)
                                    ),
                                    0
                                ) THEN ROUND(
                                    (
                                        Opt_order_qty_supp_min_order_qty / NULLIF((Consumption_per_Month / 30), 0)
                                    ),
                                    0
                                )
                                ELSE Min_Order_Interval
                            END
                        ) THEN (Consumption_per_Month / 30) * (
                            Avg_General_Replenishment_Time + 3 * Stddev_replenishment_time
                        ) + Last_Safety_Stock - CAST(
                            (
                                Avg_General_Replenishment_Time / NULLIF(Opt_order_qty_supp_min_order_qty, 0) * (Consumption_per_Month / 30)
                            ) AS INTEGER
                        ) * Opt_order_qty_supp_min_order_qty
                    END
                ),
                0
            ) AS Reporting_stock_level,
            CASE
                WHEN (
                    ROUND(
                        (
                            (
                                Last_Stock_Amount - (
                                    (Consumption_per_Month / 30) * (
                                        Avg_General_Replenishment_Time + 3 * Stddev_replenishment_time
                                    ) + Last_Safety_Stock
                                )
                            ) / NULLIF((Consumption_per_Month / 30), 0)
                        ),
                        0
                    ) < 0
                )
                AND (pur_order_doc_open = TRUE) THEN Avg_General_Replenishment_Time
                ELSE ROUND(
                    (
                        (
                            Last_Stock_Amount - (
                                (Consumption_per_Month / 30) * (
                                    Avg_General_Replenishment_Time + 3 * Stddev_replenishment_time
                                ) + Last_Safety_Stock
                            )
                        ) / NULLIF((Consumption_per_Month / 30), 0)
                    ),
                    0
                )
            END AS Time_to_next_order,
            ROUND(
                (
                    (
                        CASE
                            WHEN Min_Order_Interval < ROUND(
                                (
                                    Opt_order_qty_supp_min_order_qty / NULLIF((Consumption_per_Month / 30), 0)
                                ),
                                0
                            ) THEN ROUND(
                                (
                                    Opt_order_qty_supp_min_order_qty / NULLIF((Consumption_per_Month / 30), 0)
                                ),
                                0
                            )
                            ELSE Min_Order_Interval
                        END
                    ) + Avg_General_Replenishment_Time + Stddev_replenishment_time
                ),
                0
            ) AS Viewing_horizon
        FROM
            stock_optimization_model
    ),
    stock_opt_data_grund_menge AS (
        SELECT
            company,
            part_i_d,
            mvmt_m_r_p_area,
            Part_Group,
            Part_Type,
            part_desc1,
            mvmt_m_r_p_group,
            Last_Order_Date,
            Avg_General_Replenishment_Time,
            Last_Stock_Value,
            ROUND(Last_Stock_Amount, 0)                         AS Last_Stock_Amount,
            Last_Safety_Stock,
            Consumption_per_Month,
            SOM_BASE_INDICATOR,
            min_period_date,
            consumption_periodic,
            number_of_movements_postings_periodic,
            Delivery_Costs,
            Order_Process_Costs,
            Min_Order_Interval,
            part_supp_order_multiple,
            part_supp_min_order_qty,
            Stddev_replenishment_time, 
            Last_Unit_Price,
            CASE
                WHEN Min_Order_Interval < ROUND(
                    (
                        Opt_order_qty_supp_order_multiple / NULLIF((Consumption_per_Month / 30), 0)
                    ),
                    0
                ) THEN ROUND(
                    (
                        Opt_order_qty_supp_order_multiple / NULLIF((Consumption_per_Month / 30), 0)
                    ),
                    0
                )
                ELSE Min_Order_Interval
            END AS Order_interval,
            --
            --
            --
            --
            Opt_order_qty_supp_order_multiple AS Opt_order_qty,
            --
            ROUND(
                (
                    Opt_order_qty_supp_order_multiple + (Consumption_per_Month / 30) * (3 * Stddev_replenishment_time) + Last_Safety_Stock
                ),
                0
            ) AS Recommended_min_stock_value,
            --
            --
            --
            --
            ROUND(
                (
                    (
                        Last_Stock_Amount - (
                            Opt_order_qty_supp_order_multiple + (Consumption_per_Month / 30) * (3 * Stddev_replenishment_time) + Last_Safety_Stock
                        )
                    ) * (
                        Last_Unit_Price + (Order_Process_Costs + Delivery_Costs) / NULLIF(Opt_order_qty_supp_order_multiple, 0)
                    )
                ),
                2
            ) AS Optimization_value,
            ROUND(
                (
                    (
                        avg_stock_periodic - (
                            Opt_order_qty_supp_order_multiple + (Consumption_per_Month / 30) * (3 * Stddev_replenishment_time) + Last_Safety_Stock
                        )
                    ) * (
                        Last_Unit_Price + (Order_Process_Costs + Delivery_Costs) / NULLIF(Opt_order_qty_supp_order_multiple, 0)
                    )
                ),
                2
            ) AS Periodic_Optimization_Value,
            --
            --
            ROUND(
                (
                    CASE
                        WHEN Avg_General_Replenishment_Time <= (
                            CASE
                                WHEN Min_Order_Interval < ROUND(
                                    (
                                        Opt_order_qty_supp_order_multiple / NULLIF((Consumption_per_Month / 30), 0)
                                    ),
                                    0
                                ) THEN ROUND(
                                    (
                                        Opt_order_qty_supp_order_multiple / NULLIF((Consumption_per_Month / 30), 0)
                                    ),
                                    0
                                )
                                ELSE Min_Order_Interval
                            END
                        ) THEN (Consumption_per_Month / 30) * (
                            Avg_General_Replenishment_Time + 3 * Stddev_replenishment_time
                        ) + Last_Safety_Stock - CAST(
                            (
                                Avg_General_Replenishment_Time / NULLIF(
                                    (
                                        CASE
                                            WHEN Min_Order_Interval < ROUND(
                                                (
                                                    Opt_order_qty_supp_order_multiple / NULLIF((Consumption_per_Month / 30), 0)
                                                ),
                                                0
                                            ) THEN ROUND(
                                                (
                                                    Opt_order_qty_supp_order_multiple / NULLIF((Consumption_per_Month / 30), 0)
                                                ),
                                                0
                                            )
                                            ELSE Min_Order_Interval
                                        END
                                    ),
                                    0
                                )
                            ) AS INTEGER
                        ) * (Consumption_per_Month / 30) * (
                            CASE
                                WHEN Min_Order_Interval < ROUND(
                                    (
                                        Opt_order_qty_supp_order_multiple / NULLIF((Consumption_per_Month / 30), 0)
                                    ),
                                    0
                                ) THEN ROUND(
                                    (
                                        Opt_order_qty_supp_order_multiple / NULLIF((Consumption_per_Month / 30), 0)
                                    ),
                                    0
                                )
                                ELSE Min_Order_Interval
                            END
                        )
                        WHEN Avg_General_Replenishment_Time > (
                            CASE
                                WHEN Min_Order_Interval < ROUND(
                                    (
                                        Opt_order_qty_supp_order_multiple / NULLIF((Consumption_per_Month / 30), 0)
                                    ),
                                    0
                                ) THEN ROUND(
                                    (
                                        Opt_order_qty_supp_order_multiple / NULLIF((Consumption_per_Month / 30), 0)
                                    ),
                                    0
                                )
                                ELSE Min_Order_Interval
                            END
                        ) THEN (Consumption_per_Month / 30) * (
                            Avg_General_Replenishment_Time + 3 * Stddev_replenishment_time
                        ) + Last_Safety_Stock - CAST(
                            (
                                Avg_General_Replenishment_Time / NULLIF(Opt_order_qty_supp_order_multiple, 0) * (Consumption_per_Month / 30)
                            ) AS INTEGER
                        ) * Opt_order_qty_supp_order_multiple
                    END
                ),
                0
            ) AS Reporting_stock_level,
            CASE
                WHEN (
                    ROUND(
                        (
                            (
                                Last_Stock_Amount - (
                                    (Consumption_per_Month / 30) * (
                                        Avg_General_Replenishment_Time + 3 * Stddev_replenishment_time
                                    ) + Last_Safety_Stock
                                )
                            ) / NULLIF((Consumption_per_Month / 30), 0)
                        ),
                        0
                    ) < 0
                )
                AND (pur_order_doc_open = TRUE) THEN Avg_General_Replenishment_Time
                ELSE ROUND(
                    (
                        (
                            Last_Stock_Amount - (
                                (Consumption_per_Month / 30) * (
                                    Avg_General_Replenishment_Time + 3 * Stddev_replenishment_time
                                ) + Last_Safety_Stock
                            )
                        ) / NULLIF((Consumption_per_Month / 30), 0)
                    ),
                    0
                )
            END AS Time_to_next_order,
            ROUND(
                (
                    (
                        CASE
                            WHEN Min_Order_Interval < ROUND(
                                (
                                    Opt_order_qty_supp_order_multiple / NULLIF((Consumption_per_Month / 30), 0)
                                ),
                                0
                            ) THEN ROUND(
                                (
                                    Opt_order_qty_supp_order_multiple / NULLIF((Consumption_per_Month / 30), 0)
                                ),
                                0
                            )
                            ELSE Min_Order_Interval
                        END
                    ) + Avg_General_Replenishment_Time + Stddev_replenishment_time
                ),
                0
            ) AS Viewing_horizon
        FROM
            stock_optimization_model
    ),
    PART_FILTER_INFO AS (
        -- Part fields to add_ filter values
        SELECT
            COMPANY, -- Company              Mandant
            PART_I_D, -- PartID               Teilenummer
            mvmt_m_r_p_area, -- MRPArea              Dispobereich
            PART_DESC1, -- PartDesc1            Teilebeschreibung
            PART_GROUP, -- PartGroup            Teilegruppe
            PART_GROUP_DESC, -- PartGroupDesc        Teilegruppenbeschreibung
            PART_TYPE, -- PartType             Teileart
            PART_TYPE_DESCRIPTION, -- PartTypeDescription  Teileartenbeschreibung
            export_date_to
        FROM
            PAX_DATA
        WHERE
            part_desc1 IS NOT NULL
        GROUP BY
            company,
            part_i_d,
            mvmt_m_r_p_area,
            part_desc1,
            part_group,
            part_group_desc,
            part_type,
            part_type_description,
            export_date_to
    ),
    MODEL_RAW AS (
        SELECT
            F.company,
            F.part_i_d,
            F.part_desc1,
            F.mvmt_m_r_p_area,
            F.part_group,
            F.part_group_desc,
            F.part_type,
            F.part_type_description,
            S.mvmt_m_r_p_group,
            S.Consumption_per_Month,
            S.SOM_BASE_INDICATOR,
            S.min_period_date,
            S.consumption_periodic,
            S.number_of_movements_postings_periodic,
            S.Last_Safety_Stock,
            S.Delivery_Costs,
            S.Order_Process_Costs,
            S.Min_Order_Interval,
            S.Order_interval,
            S.Last_Unit_Price,
            S.part_supp_order_multiple,
            S.part_supp_min_order_qty,
            S.Avg_General_Replenishment_Time,
            S.Stddev_replenishment_time,
            S.Last_Stock_Amount,
            S.Opt_order_qty,
            S.Optimization_value,
            S.Periodic_Optimization_Value,
            S.Recommended_min_stock_value,
            S.Time_to_next_order,
            S.Reporting_stock_level,
            S.Viewing_horizon,
            MAX(
                ADD_DAYS (
                    F.export_date_to,
                    LEAST (S.Time_to_next_order, P.p_max_date_delta)
                )
            ) AS Next_order_point_calc,
            F.export_date_to
        FROM
            ---  SWITCH: Berechnungsbasis part_supp_order_multiple <-> MinBestMenge      stock_opt_data_best_menge <-> stock_opt_data_grund_menge
            stock_opt_data_grund_menge S
            JOIN PART_FILTER_INFO F ON S.company = F.company
            AND S.part_i_d = F.part_i_d
            AND S.mvmt_m_r_p_area = F.mvmt_m_r_p_area
            CROSS JOIN PARAMS P
        GROUP BY
            F.company,
            F.part_i_d,
            F.mvmt_m_r_p_area,
            F.part_desc1,
            F.part_group,
            F.part_group_desc,
            F.part_type,
            F.part_type_description,
            S.mvmt_m_r_p_group,
            S.Consumption_per_Month,
            S.SOM_BASE_INDICATOR,
            S.min_period_date,
            S.consumption_periodic,
            S.number_of_movements_postings_periodic,
            S.Last_Safety_Stock,
            S.Delivery_Costs,
            S.Order_Process_Costs,
            S.Min_Order_Interval,
            S.Order_interval,
            S.Last_Unit_Price,
            S.part_supp_order_multiple,
            S.part_supp_min_order_qty,
            S.Avg_General_Replenishment_Time,
            S.Stddev_replenishment_time,
            S.Last_Stock_Amount,
            S.Opt_order_qty,
            S.Optimization_value,
            S.Periodic_Optimization_Value,
            S.Recommended_min_stock_value,
            S.Time_to_next_order,
            S.Reporting_stock_level,
            S.Viewing_horizon,
            F.export_date_to
    ),
NPAAPPModel AS (
SELECT
    company,
    part_i_d,
    mvmt_m_r_p_area,
    part_desc1,
    part_group,
    part_group_desc,
    part_type,
    part_type_description,
    mvmt_m_r_p_group,
    Consumption_per_Month,
    SOM_BASE_INDICATOR,
    min_period_date,
    consumption_periodic,
    number_of_movements_postings_periodic,
    Last_Safety_Stock,
    Delivery_Costs,
    Order_Process_Costs,
    Min_Order_Interval,
    Order_interval,
    Last_Unit_Price,
    part_supp_order_multiple,
    part_supp_min_order_qty,
    Avg_General_Replenishment_Time,
    Stddev_replenishment_time,
    Last_Stock_Amount,
    Recommended_min_stock_value,
    Opt_order_qty,
    CASE
        WHEN Optimization_value < 0 THEN 0
        ELSE Optimization_value
    END AS Optimization_value, -- Show only positive otmization values for calculation
    CASE
        WHEN Periodic_Optimization_Value < 0 THEN 0
        ELSE Periodic_Optimization_Value
    END AS Periodic_Optimization_Value, -- Show only positive otmization values for calculation
    Time_to_next_order,
    Reporting_stock_level,
    Viewing_horizon,
    COALESCE(TO_VARCHAR (Next_order_point_calc), '') AS Next_order_point_calc,
    COALESCE(
        TO_VARCHAR (
            CASE --  CURRENT_DATE changed to export_date_to -> move to next stage
                WHEN Next_order_point_calc < export_date_to THEN export_date_to
                ELSE Next_order_point_calc
            END
        ),
        ''
    ) AS Next_order_point
FROM
    MODEL_RAW),
PARTS_DATA AS (
	SELECT	
		  pe.COMPANY
		, pe.PART_I_D
		, pe.PART_TYPE
		, pe.PART_SELECTION
		, pe.PART_DESC1
		, pe.PART_DESC2
		, MAX(pe.MVMT_M_R_P_GROUP) AS MVMT_M_R_P_GROUP
		, MAX(pe.MVMT_M_R_P_GROUP_DESC) AS MVMT_M_R_P_GROUP_DESC
		, pe.S_ARTIKEL_SB_PRODUKTMANAGER
		, pe.PART_REPLENISHMENT_TIME
		, MAX(nmd.PART_SUPP_MIN_ORDER_QTY) AS PART_SUPP_MIN_ORDER_QTY
	FROM
		EMZ."pa_export" pe
	JOIN emz."nemo_master_data" nmd 
		ON pe.COMPANY = nmd.COMPANY
		AND pe.PART_I_D = nmd.PART_I_D		
	WHERE pe.PART_I_D IS NOT NULL
	GROUP BY 
		  pe.COMPANY
		, pe.PART_I_D
		, pe.PART_TYPE
		, pe.PART_SELECTION
		, pe.PART_DESC1
		, pe.PART_DESC2
		, pe.S_ARTIKEL_SB_PRODUKTMANAGER
		, pe.PART_REPLENISHMENT_TIME
)
SELECT
   	  model.COMPANY
	, model.PART_I_D
	, parts.PART_TYPE
	, parts.PART_SELECTION
	, parts.PART_DESC1
	, parts.PART_DESC2
	, parts.MVMT_M_R_P_GROUP
	, parts.MVMT_M_R_P_GROUP_DESC
	, parts.S_ARTIKEL_SB_PRODUKTMANAGER
	, parts.PART_REPLENISHMENT_TIME
	, model.Avg_General_Replenishment_Time
	, model.Avg_General_Replenishment_Time - parts.PART_REPLENISHMENT_TIME AS DELTA_PART_REPLENISHMENT_TIME
	, parts.PART_SUPP_MIN_ORDER_QTY
	, model.part_supp_order_multiple
	, model.part_supp_order_multiple - parts.PART_SUPP_MIN_ORDER_QTY AS DELTA_PART_SUPP_ORDER_MULTIPLE
	, model.Optimization_value
FROM 
	NPAAPPModel model
JOIN 	PARTS_DATA parts
	ON 	parts.COMPANY  = model.COMPANY
	AND parts.PART_I_D = model.PART_I_D
where
	model.COMPANY = '70'
	AND model.PART_I_D = '20.2510.7.001'