select *
from
    (
        select *
        from
            (
                select *
                from live.fact_ar_invoice_lineitem fact_ar_invoice_lineitem
                inner join
                    live.fact_accounting_document_header fact_accounting_document_header_sap
                    on fact_ar_invoice_lineitem.legal_entity
                    = fact_accounting_document_header_sap.le_number
                    and fact_ar_invoice_lineitem.document_number
                    = fact_accounting_document_header_sap.document_number
                    and fact_ar_invoice_lineitem.fiscal_year
                    = fact_accounting_document_header_sap.fiscal_year
                where
                    fact_ar_invoice_lineitem.src_system_name = 'sap'
                    and ifnull(fact_ar_invoice_lineitem.slt_delete, '') <> 'X'
                    and fact_accounting_document_header_sap.invoice_type
                    not in ('31', '32', '33', '34', '35', '36', 'NA', 'RV', 'SD', 'ZC')

                union all

                select *
                from live.fact_ar_invoice_billing_items fact_ar_invoice_billing_items
                inner join
                    (
                        select
                            count(billing_document_item) as count_billing_doc,
                            billing_doc
                        from live.fact_ar_invoice_billing_items
                        where src_system_name = 'sap' and ifnull(slt_delete, '') <> 'X'
                        group by billing_doc
                    ) b
                    on fact_ar_invoice_billing_items.billing_doc = b.billing_doc
                inner join
                    (
                        select *
                        from
                            (
                                select *
                                from
                                    live.fact_ar_invoice_lineitem fact_ar_invoice_lineitem2
                                where
                                    ifnull(fact_ar_invoice_lineitem2.slt_delete, '')
                                    <> 'X'
                                    and fact_ar_invoice_lineitem2.src_system_name
                                    = 'sap'
                                group by
                                    fact_ar_invoice_lineitem2.billing_doc,
                                    fact_ar_invoice_lineitem2.document_number,
                                    fact_ar_invoice_lineitem2.src_system_name
                            ) fact_ar_invoice_lineitem3
    
                        inner join
                            (
                                select *
                                from
                                    live.fact_ar_invoice_lineitem fact_ar_invoice_lineitem1
                                where
                                    ifnull(fact_ar_invoice_lineitem1.slt_delete, '')
                                    <> 'X'
                                    and fact_ar_invoice_lineitem1.src_system_name
                                    = 'sap'
                            ) fact_ar_invoice_lineitem4
                   
                            on fact_ar_invoice_lineitem4.billing_doc
                            = fact_ar_invoice_lineitem3.billing_doc
                            and fact_ar_invoice_lineitem4.document_number
                            = fact_ar_invoice_lineitem3.document_number
                            and fact_ar_invoice_lineitem4.rank1 = 1
                    ) fact_ar_invoice_lineitem
                    on fact_ar_invoice_billing_items.billing_doc
                    = fact_ar_invoice_lineitem.billing_doc
                    and fact_ar_invoice_billing_items.billing_doc
                    = fact_ar_invoice_lineitem.document_number
                    and fact_ar_invoice_lineitem.src_system_name
                    = fact_ar_invoice_billing_items.src_system_name
                inner join
                    live.fact_accounting_document_header fact_accounting_document_header_sap
                    on fact_ar_invoice_lineitem.legal_entity
                    = fact_accounting_document_header_sap.le_number
                    and fact_ar_invoice_lineitem.document_number
                    = fact_accounting_document_header_sap.document_number
                    and fact_ar_invoice_lineitem.fiscal_year
                    = fact_accounting_document_header_sap.fiscal_year

                where
                    -- fact_AR_INVOICE_BILLING_ITEMS.src_system_name='sap'
                    ifnull(fact_ar_invoice_billing_items.slt_delete, '') <> 'X'
            ) fact

        left join
            (
                select
                    a.fiscal_year,
                    a.legal_entity,
                    a.document_number,
                    min(document_line_number) as min_document_line_number
                from
                    (
                        select *
                        from live.fact_ar_invoice_lineitem f1
                        inner join
                            live.fact_accounting_document_header fact_accounting_document_header_sap
                            on f1.legal_entity
                            = fact_accounting_document_header_sap.le_number
                            and f1.document_number
                            = fact_accounting_document_header_sap.document_number
                            and f1.fiscal_year
                            = fact_accounting_document_header_sap.fiscal_year
                            and f1.src_system_name
                            = fact_accounting_document_header_sap.src_system_name_s
                            and ifnull(
                                fact_accounting_document_header_sap.slt_delete_s, ''
                            )
                            <> 'X'
                        where
                            f1.src_system_name = 'sap'
                            and ifnull(f1.slt_delete, '') <> 'X'
                            and fact_accounting_document_header_sap.invoice_type
                            not in (
                                '31',
                                '32',
                                '33',
                                '34',
                                '35',
                                '36',
                                'NA',
                                'RV',
                                'SD',
                                'ZC'
                            )
                    ) a
                group by a.fiscal_year, a.legal_entity, a.document_number
   
            ) fact2
            on fact.legal_entity = fact2.legal_entity
            and fact.fiscal_year = fact2.fiscal_year
            and fact.document_number = fact2.document_number
        left join
            (
                select
                    s3.fiscal_year,
                    s3.le_number,
                    s3.document_number,
                    s3.discount_taken_unearned,
                    s4.line_item
                from
                    (
                        select *
                        from live.fact_accounting_document_line_items
                        where
                            account_type = 'S'
                            and general_ledger_account = '0004010015'  -- If line item is S= G/L line and  G/L account is 0004010015 which is for Unearned Cash Disc Allowed
                            and ifnull(slt_delete_s, '') <> 'X'
                        group by fiscal_year, le_number, document_number
                    ) s3
                inner join
                    (
                        select
                            fiscal_year,
                            le_number,
                            document_number,
                            max(line_item) as line_item
                        from live.fact_accounting_document_line_items
                        where ifnull(slt_delete_s, '') <> 'X' and account_type = 'D'  -- Account Type D= Customers line item
                        group by fiscal_year, le_number, document_number
                    ) s4
                    on s3.le_number = s4.le_number
                    and s3.fiscal_year = s4.fiscal_year
                    and s3.document_number = s4.document_number
            ) fact_accounting_document_line_items_sap
            on fact.legal_entity = fact_accounting_document_line_items_sap.le_number
            and fact.fiscal_year = fact_accounting_document_line_items_sap.fiscal_year
            and fact.document_number
            = fact_accounting_document_line_items_sap.document_number
            and fact.document_line_number
            = fact_accounting_document_line_items_sap.line_item
        left join
            live.fact_accounting_document_line_items fact_accounting_document_line_items_sap1
            on fact.legal_entity = fact_accounting_document_line_items_sap1.le_number
            and fact.fiscal_year = fact_accounting_document_line_items_sap1.fiscal_year
            and fact.document_number
            = fact_accounting_document_line_items_sap1.document_number
            and fact.document_line_number
            = fact_accounting_document_line_items_sap1.line_item
            and fact.customer = fact_accounting_document_line_items_sap1.customer_number
        left join
            (
                select
                    fiscal_year,
                    le_number,
                    document_number,
                    sum(
                        case
                            when debit_credit_ind = 'H'
                            then (-1 * lc2_base_amount)
                            else lc2_base_amount
                        end
                    ) as exchange_rate_difference
                from live.fact_accounting_document_line_items
                where
                    account_type = 'S'  -- Account type S= G/L ACCOUNT Line
                    and transaction_key = 'KDF'  -- Transaction key for Exchange rate difference for foreign currency balances  
                    and ifnull(slt_delete_s, '') <> 'X'
                group by fiscal_year, le_number, document_number
            ) fact_accounting_document_line_items_sap2
            on fact.legal_entity = fact_accounting_document_line_items_sap2.le_number
            and fact.fiscal_year = fact_accounting_document_line_items_sap2.fiscal_year
            and fact.document_number
            = fact_accounting_document_line_items_sap2.document_number
        left join
            (
                select
                    fiscal_year,
                    le_number,
                    document_number,
                    sum(
                        case
                            when debit_credit_ind = 'H'  -- H= Credit Line
                            then (-1 * lc_amount)
                            else lc_amount
                        end
                    ) as freight_amount
                from live.fact_accounting_document_line_items
                where
                    account_type = 'S'  -- Account type S= G/L ACCOUNT Line
                    and general_ledger_account = '0004020005'  -- If G/L ACCOUNT =0004020005 =Freight Out Expense
                    and ifnull(slt_delete_s, '') <> 'X'
                group by fiscal_year, le_number, document_number
            ) fact_accounting_document_line_items_sap3
            on fact.legal_entity = fact_accounting_document_line_items_sap3.le_number
            and fact.fiscal_year = fact_accounting_document_line_items_sap3.fiscal_year
            and fact.document_number
            = fact_accounting_document_line_items_sap3.document_number
        left join
            (
                select
                    s7.fiscal_year,
                    s7.le_number,
                    s7.document_number,
                    s7.cash_tolerance,
                    s8.line_item
                from
                    (
                        select
                            fiscal_year,
                            le_number,
                            document_number,
                            sum(
                                case
                                    when debit_credit_ind = 'H'  -- H = Credit Line
                                    then (-1 * lc_amount)
                                    else lc_amount
                                end
                            ) as cash_tolerance
                        from live.fact_accounting_document_line_items
                        where
                            account_type = 'S'  -- Account type S= G/L ACCOUNT Line
                            and general_ledger_account = '0004010020'  -- If G/L ACCOUNT number 0004010020 which is for Cash Tolerance
                            and ifnull(slt_delete_s, '') <> 'X'
                        group by fiscal_year, le_number, document_number
                    ) s7
                inner join
                    (
                        select
                            fiscal_year,
                            le_number,
                            document_number,
                            max(line_item) as line_item
                        from live.fact_accounting_document_line_items
                        where ifnull(slt_delete_s, '') <> 'X' and account_type = 'D'  -- Account type D= Customers
                        group by fiscal_year, le_number, document_number
                    ) s8
                    on s7.le_number = s8.le_number
                    and s7.fiscal_year = s8.fiscal_year
                    and s7.document_number = s8.document_number
            ) fact_accounting_document_line_items_sap4
            on fact.legal_entity = fact_accounting_document_line_items_sap4.le_number
            and fact.fiscal_year = fact_accounting_document_line_items_sap4.fiscal_year
            and fact.document_number
            = fact_accounting_document_line_items_sap4.document_number
            and fact.document_line_number
            = fact_accounting_document_line_items_sap4.line_item
        left join
            live.dim_customer dim_customer
            on fact.customer = dim_customer.party_number_s
            and fact.legal_entity = dim_customer.le_number_s
            and fact.src_system_name = dim_customer.src_system_name_s
        left join
            (
                select distinct
                    le_number,
                    le_name,
                    site_name,
                    sap_company_code,
                    sap_profit_center,
                    site,
                    country,
                    region,
                    ar_credit_office,
                    support_center
                from fdh.slt_raw.dim_xxetn_map_unit_sap_ora
                where ifnull(sap_company_code, '') <> '' and active_flag = 'Y'
            ) xxetn_map_unit_sap_ora
            on fact.legal_entity = xxetn_map_unit_sap_ora.sap_company_code
            and fact.site = xxetn_map_unit_sap_ora.sap_profit_center
        left join
            fdh.slt_raw.dim_bu_heirarchy dim_bu_heirarchy
            on xxetn_map_unit_sap_ora.site = dim_bu_heirarchy.ledger
            and dim_bu_heirarchy.active_flag = 'Y'
        left join
            (
                select
                    min(ifnull(dim_payment_terms1.min_day_limit_s, 0)) over (
                        partition by dim_payment_terms1.term_id_s
                    ) mindaylimitbypartition,
                    min(ifnull(dim_payment_terms1.max_day_limit_s, 0)) over (
                        partition by dim_payment_terms1.term_id_s
                    ) maxdaylimitbypartition,
                    dim_payment_terms2.count_of_date_type,
                    dim_payment_terms1.*
                from live.dim_payment_terms dim_payment_terms1
                inner join
                    (
                        select
                            count(distinct date_type) as count_of_date_type, term_id_s
                        from live.dim_payment_terms paymentterms_dim
                        where
                            acct_type_s = 'D'
                            and ifnull(slt_delete, '') <> 'X'
                            and active_flag_s = 'Y'
                        group by term_id_s
                    ) dim_payment_terms2
                    on dim_payment_terms1.term_id_s = dim_payment_terms2.term_id_s
                where
                    dim_payment_terms1.acct_type_s = 'D'  -- The payment terms for Customers =Account type D
                    and ifnull(dim_payment_terms1.slt_delete, '') <> 'X'
                    and dim_payment_terms1.active_flag_s = 'Y'
            ) dim_payment_terms
            on fact.pay_term = dim_payment_terms.term_id_s
            and case
                when count_of_date_type > 1
                then day(fact.baseline_date)
                when
                    (
                        date_type = 'D'
                        and mindaylimitbypartition = 0
                        and maxdaylimitbypartition = 0
                    )
                then 0  -- D= Posting Date
                when (date_type = 'D')
                then day(fact.posting_date)
                when
                    (
                        date_type = 'B'
                        and mindaylimitbypartition = 0
                        and maxdaylimitbypartition = 0
                    )
                then 0  -- B= Document date
                when (date_type = 'B')
                then day(fact.document_date)
                when
                    (
                        date_type = 'C'
                        and mindaylimitbypartition = 0
                        and maxdaylimitbypartition = 0
                    )
                then 0
                when (date_type = 'C')
                then day(fact.entered_on)
                else 0
            end
            between (
                case
                    when (date_type = 'D' and ifnull(min_day_limit_s, 0) = 0)
                    then 0
                    when (date_type = 'B' and ifnull(min_day_limit_s, 0) = 0)
                    then 0
                    when (date_type = 'C' and ifnull(min_day_limit_s, 0) = 0)
                    then 0
                    else min_day_limit_s
                end
            ) and (
                case
                    when (date_type = 'D' and ifnull(max_day_limit_s, 0) = 0)
                    then 0
                    when (date_type = 'B' and ifnull(max_day_limit_s, 0) = 0)
                    then 0
                    when (date_type = 'C' and ifnull(max_day_limit_s, 0) = 0)
                    then 0
                    else max_day_limit_s
                end
            )
    ) a
where a.fiscal_year >= 2022
;
