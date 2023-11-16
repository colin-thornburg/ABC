select *
from
    (

        select

            'sap' src_system_name,
            fact.legal_entity as sap_company_code,
            fact.indicator_spl_gl as indicator_spl_gl,
            fact.receipt_number as receipt_number,
            fact.fiscal_year as fiscal_year,
            fact.document_number as document_number,
            fact.document_line_number as document_line_number,
            fact.posting_date as posting_date,
            fact.document_date as document_date,
            fact.entered_on as entered_on,
            fact.invoice_currency as invoice_currency,
            case
                when fact.tax_code = '**' then fact.tax_code1 else fact.tax_code
            end as tax_code,  -- if there is a value in tax code then that has to be picked up
            (
                case
                    when
                        fact.local_currency in (
                            'CRC',
                            'HUF',
                            'JPY',
                            'KZT',
                            'PKR',
                            'COP',
                            'IDR',
                            'KRW',
                            'NTD',
                            'VND'
                        )
                    then 100

                    when fact.local_currency in ('BYB', 'LBP', 'ROL')
                    then 1000

                    when fact.local_currency in ('TRL')
                    then 100000

                    when fact.local_currency in ('BHD', 'OMR', 'JOD')
                    then 0.1
                    else 1
                end
            )
            * fact.amount_in_lc as amount_in_lc,
            (
                case
                    when
                        fact.currency in (
                            'CRC',
                            'HUF',
                            'JPY',
                            'KZT',
                            'PKR',
                            'COP',
                            'IDR',
                            'KRW',
                            'NTD',
                            'VND'
                        )
                    then 100

                    when fact.currency in ('BYB', 'LBP', 'ROL')
                    then 1000

                    when fact.currency in ('TRL')
                    then 100000

                    when fact.currency in ('BHD', 'OMR', 'JOD')
                    then 0.1
                    else 1
                end
            )
            * fact.amount_in_gc as amount_in_dc,
            fact.tax_original as tax_original,
            fact.line_description as line_description,
            fact.gl_account_1 as gl_account,
            fact.baseline_date as baseline_date,
            fact.pay_term as pay_term,
            fact.invoice_ref as invoice_ref,
            fact.tax_amt_tax_curr as tax_amount,
            fact.tax_amt_tax_curr as tax_amt_tax_curr,
            fact.trading_partner as trading_partner,
            case
                when
                    fact.dr_cr_ind = 'H'
                    and fact.document_type in ('RV', 'DZ')
                    and fact.baseline_date is not null
                then fact.baseline_date
                else

                    (
                        case
                            when fact.baseline_date is not null
                            then
                                date_add(
                                    to_date(fact.baseline_date, 'dd-MM-yyyy'),

                                    cast(
                                        (
                                            case
                                                when fact.net_payment_terms_period <> 0
                                                then fact.net_payment_terms_period
                                                when fact.days_2 <> 0
                                                then fact.days_2
                                                else fact.days_1
                                            end
                                        ) as int
                                    )
                                )
                            else null
                        end
                    )
            end as due_date,
            fact.tax_registration_number as tax_registration_number,
            fact.dest_country as dest_country,
            fact.reason_code as reason_code,
            fact.lc2_amount as amount_in_gc,
            -- If the clearing date is null means the Invoice is still open and note
            -- yet cleared ,hence the status will be open .
            case
                when fact.clearing_date is null then 'Open' else 'Closed'
            end as invoice_status,
            fact.invoice_ref_1 as invoice_ref_1,
            fact.sales_order as sales_order,
            fact.sales_order_line as sales_order_line,
            fact.activity_center as activity_center,
            fact.site as sap_profit_center,
            fact.payment_ref as payment_ref,
            fact.functional_area as functional_area,
            case
                when fact_accounting_document_line_items_sap1.debit_credit_ind = 'S'
                then (-1 * fact_accounting_document_line_items_sap1.discount_amount_dc)

                else fact_accounting_document_line_items_sap1.discount_amount_dc
            end as cash_discount,
            fact.document_type as document_type,
            case
                when fact.posting_period in ('13', '14', '15', '16')
                then '12'
                else fact.posting_period
            end as posting_period,  -- If period is greater than 12 then consider as period 12 only
            fact.reference as reference,
            fact.reversal_document as reversal_document,
            fact.reversal_year as reversal_year,
            fact.header_text as header_text,
            fact.exchange_rate as exchange_rate,
            fact.reference_key as reference_key,
            fact.local_currency as local_currency,
            fact.local_currency_2 as group_currency,
            fact.reversal_flag as reversal_flag,
            fact.reversal_date as reversal_date,
            fact.reversal_ind as reversal_ind,
            fact.tax_amt as tax_base_amount,
            fact.lc_tax as tax_amount_lc,
            fact.customer as account_number,
            dim_customer.party_name_s as account_name,
            dim_customer.city_s as customer_city,
            dim_customer.state_s as customer_state,
            dim_customer.account_group_s as customer_account_group,
            dim_customer.county_code_s as county_code,
            dim_customer.trading_partner_s as customer_trading_partner,
            dim_customer.deletion_flag_s as del_flag_customer,
            dim_customer.alternat_payer_s as alternate_payer,
            dim_customer.payment_term_id_s as payment_term_id,
            dim_customer.country_s as customer_country,
            dim_customer.vat_registration_number_s as vat_reg_number,
            dim_customer.total_limit_s as customer_tot_limit,
            dim_customer.individ_limit_s as customer_ind_limit,
            dim_customer.sales_channel_code_s as sales_channel_code,
            dim_payment_terms.name_s as payment_term_description,
            case
                when fact.clearing_date is null
                then fact.amount_in_gc

                when fact.clearing_date is not null
                then 0
            end as amount_due_remaining_dc,
            case
                when fact.clearing_date is null
                then fact.lc2_amount

                when fact.clearing_date is not null
                then 0
            end as amount_due_remaining_gc,
            fact.amount_in_gc as amount,
            fact_accounting_document_line_items_sap.discount_taken_unearned
            as discount_unearned,
            fact_accounting_document_line_items_sap4.cash_tolerance
            as adjustment_amount_dc,
            fact.clearing_date as apply_date,
            dim_customer.duns_no_s as duns_number,
            case
                when dim_customer.account_group_s = 'Z300' then 'I' else 'R'
            end as account_type,  -- Z300 = Customer account group for Inter Company (I) otherwise R (third party).
            case
                when fact.net_payment_terms_period <> 0
                then fact.net_payment_terms_period

                when fact.days_2 <> 0
                then fact.days_2

                else fact.days_1
            end as days_from_baseline_date1,
            fact_accounting_document_line_items_sap2.exchange_rate_difference
            as exchange_rate_difference,
            case
                when fact.document_line_number = fact2.min_document_line_number
                then fact_accounting_document_line_items_sap3.freight_amount
                else null
            end as freight_amount_dc,
            fact_accounting_document_line_items_sap4.cash_tolerance as cash_tolerance,
            fact_accounting_document_line_items_sap3.freight_amount as freight_amount_lc  -- This is about the freight charges if in USD currency then the fields which needs to be picked up if not then to convert it to Local currenct using the conversion rate.

            ,
            case
                when fact.document_line_number = fact2.min_document_line_number
                then fact_accounting_document_line_items_sap3.freight_amount

                else null
            end as freight_amount_gc,  -- When Invoice currency is = USD then freight amount is to be picked up directly from mentioned fields
            case
                when fact.invoice_currency = 'USD'
                then fact_accounting_document_line_items_sap4.cash_tolerance

                else fact_accounting_document_line_items_sap4.cash_tolerance
            end as adjustment_amount_lc,
            case
                when fact.invoice_currency = 'USD'
                then fact_accounting_document_line_items_sap4.cash_tolerance

                else fact_accounting_document_line_items_sap4.cash_tolerance
            end as adjustment_amount_gc,
            getdate() as modify_date,
            'D' as account_type_lineitem,  -- Account type D= CUSTOMERS
            dim_bu_heirarchy.ledger_level1,
            dim_bu_heirarchy.ledger_level_description1,
            dim_bu_heirarchy.ledger_level2,
            dim_bu_heirarchy.ledger_level_description2,
            dim_bu_heirarchy.ledger_level3,
            dim_bu_heirarchy.ledger_level_description3,
            dim_bu_heirarchy.ledger_level4,
            dim_bu_heirarchy.ledger_level_description4,
            dim_bu_heirarchy.ledger_level5,
            dim_bu_heirarchy.ledger_level_description5,
            dim_bu_heirarchy.ledger_level6,
            dim_bu_heirarchy.ledger_level_description6,
            dim_bu_heirarchy.ledger_level7,
            dim_bu_heirarchy.ledger_level_description7,
            dim_bu_heirarchy.ledger_level8,
            dim_bu_heirarchy.ledger_level_description8,
            dim_bu_heirarchy.ledger_level9,
            dim_bu_heirarchy.ledger_level_description9,
            dim_bu_heirarchy.ledger_level10,
            dim_bu_heirarchy.ledger_level_description10,
            dim_bu_heirarchy.ledger_level11,
            dim_bu_heirarchy.ledger_level_description11,
            dim_bu_heirarchy.ledger_level12,
            dim_bu_heirarchy.ledger_level_description12,
            dim_bu_heirarchy.ledger_level13,
            dim_bu_heirarchy.ledger_level_description13,
            dim_bu_heirarchy.ledger_level14,
            dim_bu_heirarchy.ledger_level_description14,
            dim_bu_heirarchy.ledger_level15,
            dim_bu_heirarchy.ledger_level_description15,
            dim_bu_heirarchy.ledger_level16,
            dim_bu_heirarchy.ledger_level_description16,
            dim_bu_heirarchy.ledger_level17,
            dim_bu_heirarchy.ledger_level_description17,
            dim_bu_heirarchy.ledger_level18,
            dim_bu_heirarchy.ledger_level_description18,
            dim_bu_heirarchy.ledger_level19,
            dim_bu_heirarchy.ledger_level_description19,
            dim_bu_heirarchy.ledger_level20,
            dim_bu_heirarchy.ledger_level_description20

        from

            (
                select

                    fact_ar_invoice_lineitem.billing_doc as billing_doc,
                    fact_ar_invoice_lineitem.legal_entity as legal_entity,
                    fact_ar_invoice_lineitem.customer as customer,
                    fact_ar_invoice_lineitem.indicator_spl_gl as indicator_spl_gl,
                    fact_ar_invoice_lineitem.clearing_date as clearing_date,
                    fact_ar_invoice_lineitem.receipt_number as receipt_number,
                    fact_ar_invoice_lineitem.fiscal_year as fiscal_year,
                    fact_ar_invoice_lineitem.document_number as document_number,
                    fact_ar_invoice_lineitem.document_line_number
                    as document_line_number,
                    fact_ar_invoice_lineitem.posting_date as posting_date,
                    fact_ar_invoice_lineitem.trx_date as document_date,
                    fact_ar_invoice_lineitem.entered_on as entered_on,
                    fact_ar_invoice_lineitem.invoice_currency as invoice_currency,
                    fact_ar_invoice_lineitem.tax_code as tax_code,
                    fact_ar_invoice_lineitem.tax_code1_s as tax_code1,
                    fact_ar_invoice_lineitem.line_description as line_description,
                    fact_ar_invoice_lineitem.natural_account as gl_account,
                    fact_ar_invoice_lineitem.natural_account_1 as gl_account_1,
                    fact_ar_invoice_lineitem.baseline_date as baseline_date,
                    fact_ar_invoice_lineitem.pay_term as pay_term,
                    fact_ar_invoice_lineitem.invoice_ref as invoice_ref,
                    fact_ar_invoice_lineitem.trading_partner as trading_partner,
                    fact_ar_invoice_lineitem.due_date as due_date,
                    fact_ar_invoice_lineitem.tax_registration_number
                    as tax_registration_number,
                    fact_ar_invoice_lineitem.dest_country as dest_country,
                    fact_ar_invoice_lineitem.reason_code as reason_code,
                    fact_ar_invoice_lineitem.invoice_status as invoice_status,
                    fact_ar_invoice_lineitem.invoice_ref_1 as invoice_ref_1,
                    fact_ar_invoice_lineitem.sales_documnet as sales_order,
                    fact_ar_invoice_lineitem.sales_order_line as sales_order_line,
                    fact_ar_invoice_lineitem.activity_center as activity_center,
                    fact_ar_invoice_lineitem.site as site,
                    fact_ar_invoice_lineitem.payment_ref as payment_ref,
                    fact_ar_invoice_lineitem.functional_area as functional_area,
                    fact_ar_invoice_lineitem.net_payment_terms_period,
                    fact_ar_invoice_lineitem.days_1,
                    fact_ar_invoice_lineitem.days_2,
                    fact_ar_invoice_lineitem.src_system_name,
                    fact_ar_invoice_lineitem.dr_cr_ind,
                    case
                        when fact_ar_invoice_lineitem.dr_cr_ind = 'H'
                        then (-1 * fact_ar_invoice_lineitem.amount_in_lc)
                        else fact_ar_invoice_lineitem.amount_in_lc
                    end as amount_in_lc,
                    case
                        when fact_ar_invoice_lineitem.dr_cr_ind = 'H'
                        then (-1 * fact_ar_invoice_lineitem.amount_in_gc)
                        else fact_ar_invoice_lineitem.amount_in_gc
                    end as amount_in_gc,
                    case
                        when fact_ar_invoice_lineitem.dr_cr_ind = 'H'
                        then (-1 * fact_ar_invoice_lineitem.lc_tax)
                        else fact_ar_invoice_lineitem.lc_tax
                    end as lc_tax,
                    case
                        when fact_ar_invoice_lineitem.dr_cr_ind = 'H'
                        then (-1 * fact_ar_invoice_lineitem.tax_original)
                        else fact_ar_invoice_lineitem.tax_original
                    end as tax_original,
                    case
                        when fact_ar_invoice_lineitem.dr_cr_ind = 'H'
                        then (-1 * fact_ar_invoice_lineitem.tax_amt)
                        else fact_ar_invoice_lineitem.tax_amt
                    end as tax_amt,
                    case
                        when fact_ar_invoice_lineitem.dr_cr_ind = 'H'
                        then (-1 * fact_ar_invoice_lineitem.tax_amt_tax_curr)
                        else fact_ar_invoice_lineitem.tax_amt_tax_curr
                    end as tax_amt_tax_curr,
                    case
                        when fact_ar_invoice_lineitem.dr_cr_ind = 'H'
                        then (-1 * fact_ar_invoice_lineitem.tax_original_in_curr1)
                        else fact_ar_invoice_lineitem.tax_original_in_curr1
                    end as tax_original_in_curr1,
                    case
                        when fact_ar_invoice_lineitem.dr_cr_ind = 'H'
                        then (-1 * fact_ar_invoice_lineitem.tax_original_in_curr2)
                        else fact_ar_invoice_lineitem.tax_original_in_curr2
                    end as tax_original_in_curr2,
                    case
                        when fact_ar_invoice_lineitem.dr_cr_ind = 'H'
                        then (-1 * fact_ar_invoice_lineitem.lc2_amount)
                        else fact_ar_invoice_lineitem.lc2_amount
                    end as lc2_amount,
                    fact_accounting_document_header_sap.invoice_type as document_type,
                    fact_accounting_document_header_sap.period as posting_period,
                    fact_accounting_document_header_sap.reference_number as reference,
                    fact_accounting_document_header_sap.parent_reversal_id
                    as reversal_document,
                    fact_accounting_document_header_sap.year as reversal_year,
                    fact_accounting_document_header_sap.invoice_description
                    as header_text,
                    fact_accounting_document_header_sap.invoice_currency_code
                    as currency,
                    fact_accounting_document_header_sap.local_currency as local_currency

                    ,
                    fact_accounting_document_header_sap.exchange_rate as exchange_rate,
                    fact_accounting_document_header_sap.le_number as le_number,
                    fact_accounting_document_header_sap.reference_key as reference_key,
                    fact_accounting_document_header_sap.local_currency2
                    as local_currency_2,
                    fact_accounting_document_header_sap.reversal_flag as reversal_flag,
                    fact_accounting_document_header_sap.reverse_posting_date
                    as reversal_date,
                    fact_accounting_document_header_sap.reversal_indicator
                    as reversal_ind,
                    fact_accounting_document_header_sap.ledger as ledger

                from live.fact_ar_invoice_lineitem fact_ar_invoice_lineitem

                inner join
                    live.fact_accounting_document_header fact_accounting_document_header_sap

                    on fact_ar_invoice_lineitem.legal_entity
                    = fact_accounting_document_header_sap.le_number

                    and fact_ar_invoice_lineitem.document_number
                    = fact_accounting_document_header_sap.document_number

                    and fact_ar_invoice_lineitem.fiscal_year
                    = fact_accounting_document_header_sap.fiscal_year

                -- and  fact_ar_invoice_lineitem.src_system_name=
                -- FACT_ACCOUNTING_DOCUMENT_HEADER_sap.src_system_name_s and
                -- ifnull(FACT_ACCOUNTING_DOCUMENT_HEADER_sap.slt_delete_s,'')<>'X'
                where
                    fact_ar_invoice_lineitem.src_system_name = 'sap'

                    and ifnull(fact_ar_invoice_lineitem.slt_delete, '') <> 'X'

                    and fact_accounting_document_header_sap.invoice_type
                    not in ('31', '32', '33', '34', '35', '36', 'NA', 'RV', 'SD', 'ZC')

                /*Invocie type should not be '31'GUI Cust_3 copies,'32'GUI Cust_2 copies,'33'GUI Cust_3 copies CN,'34'GUI Cust_2 copies CN,'35'E-GUI Cust_3 copies,

'36'GUI Cust_ZeroTax Frg,'NA'Adjustment Notes,'RV'  Billing doc.transfer,'SD'Support Documents,'ZC'RO local billing doc*/
                union all

                -- In this union we are not considering certain Invocie type should
                -- not be '31'GUI Cust_3 copies,'32'GUI Cust_2 copies,'33'GUI Cust_3
                -- copies CN,'34'GUI Cust_2 copies CN,'35'E-GUI Cust_3 copies,'36'GUI
                -- Cust_ZeroTax Frg,'NA'Adjustment Notes,'RV'  Billing
                -- doc.transfer,'SD'Support Documents,'ZC'RO local billing doc
                select

                    fact_ar_invoice_billing_items.billing_doc as billing_doc,
                    fact_ar_invoice_lineitem.legal_entity,
                    fact_ar_invoice_lineitem.customer,
                    fact_ar_invoice_lineitem.indicator_spl_gl,
                    fact_ar_invoice_lineitem.clearing_date,
                    fact_ar_invoice_lineitem.receipt_number,
                    fact_ar_invoice_lineitem.fiscal_year,
                    fact_ar_invoice_lineitem.document_number,
                    fact_ar_invoice_billing_items.billing_document_item
                    as document_line_number,
                    fact_ar_invoice_lineitem.posting_date,
                    fact_ar_invoice_lineitem.document_date,
                    fact_ar_invoice_lineitem.entered_on,
                    fact_ar_invoice_lineitem.invoice_currency,
                    fact_ar_invoice_lineitem.tax_code,
                    fact_ar_invoice_lineitem.tax_code1 as tax_code1,
                    fact_ar_invoice_billing_items.description as line_description,
                    fact_ar_invoice_lineitem.gl_account,
                    fact_ar_invoice_lineitem.gl_account_1,
                    fact_ar_invoice_lineitem.baseline_date,
                    fact_ar_invoice_lineitem.pay_term as pay_term,
                    fact_ar_invoice_lineitem.invoice_ref,
                    fact_ar_invoice_lineitem.trading_partner,
                    fact_ar_invoice_lineitem.due_date,
                    fact_ar_invoice_lineitem.tax_registration_number,
                    fact_ar_invoice_lineitem.dest_country,
                    fact_ar_invoice_lineitem.reason_code,
                    fact_ar_invoice_lineitem.invoice_status,
                    fact_ar_invoice_lineitem.invoice_ref_1,
                    fact_ar_invoice_billing_items.sales_order as sales_order,
                    fact_ar_invoice_billing_items.sales_order_item as sales_order_line,
                    fact_ar_invoice_lineitem.activity_center,
                    fact_ar_invoice_lineitem.site,
                    fact_ar_invoice_lineitem.payment_ref,
                    fact_ar_invoice_lineitem.functional_area,
                    fact_ar_invoice_lineitem.net_payment_terms_period,
                    fact_ar_invoice_lineitem.days_1,
                    fact_ar_invoice_lineitem.days_2,
                    fact_ar_invoice_lineitem.src_system_name,
                    fact_ar_invoice_lineitem.dr_cr_ind,
                    -- Dvivde the amounts by count of lineitems for each billing doc..
                    -- Amount is picked from BSID/AD
                    (
                        fact_ar_invoice_lineitem.amount_in_lc / b.count_billing_doc
                    ) as amount_in_lc,
                    (
                        fact_ar_invoice_lineitem.amount_in_gc / b.count_billing_doc
                    ) as amount_in_gc,
                    (fact_ar_invoice_lineitem.lc_tax / b.count_billing_doc) as lc_tax,
                    (
                        fact_ar_invoice_lineitem.tax_original / b.count_billing_doc
                    ) as tax_original,
                    (fact_ar_invoice_lineitem.tax_amt / b.count_billing_doc) as tax_amt,
                    (
                        fact_ar_invoice_lineitem.tax_amt_tax_curr / b.count_billing_doc
                    ) as tax_amt_tax_curr,
                    (
                        fact_ar_invoice_lineitem.tax_original_in_curr1
                        / b.count_billing_doc
                    ) as tax_original_in_curr1,
                    (
                        fact_ar_invoice_lineitem.tax_original_in_curr2
                        / b.count_billing_doc
                    ) as tax_original_in_curr2,
                    (
                        fact_ar_invoice_lineitem.lc2_amount / b.count_billing_doc
                    ) as lc2_amount,
                    fact_accounting_document_header_sap.invoice_type as document_type,
                    fact_accounting_document_header_sap.period as posting_period,
                    fact_accounting_document_header_sap.reference_number as reference,
                    fact_accounting_document_header_sap.parent_reversal_id
                    as reversal_document,
                    fact_accounting_document_header_sap.year as reversal_year,
                    fact_accounting_document_header_sap.invoice_description
                    as header_text,
                    fact_accounting_document_header_sap.invoice_currency_code
                    as currency,
                    fact_accounting_document_header_sap.local_currency as local_currency

                    ,
                    fact_accounting_document_header_sap.exchange_rate as exchange_rate,
                    fact_accounting_document_header_sap.le_number as le_number,
                    fact_accounting_document_header_sap.reference_key as reference_key,
                    fact_accounting_document_header_sap.local_currency2
                    as local_currency_2,
                    fact_accounting_document_header_sap.reversal_flag as reversal_flag,
                    fact_accounting_document_header_sap.reverse_posting_date
                    as reversal_date,
                    fact_accounting_document_header_sap.reversal_indicator
                    as reversal_ind,
                    fact_accounting_document_header_sap.ledger as ledger

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
                        select
                            fact_ar_invoice_lineitem4.legal_entity,
                            fact_ar_invoice_lineitem4.customer,
                            fact_ar_invoice_lineitem4.indicator_spl_gl,
                            fact_ar_invoice_lineitem4.clearing_date,
                            fact_ar_invoice_lineitem4.receipt_number,
                            fact_ar_invoice_lineitem4.fiscal_year,
                            fact_ar_invoice_lineitem4.document_number,
                            fact_ar_invoice_lineitem4.billing_doc,
                            fact_ar_invoice_lineitem4.posting_date,
                            fact_ar_invoice_lineitem4.document_date,
                            fact_ar_invoice_lineitem4.entered_on,
                            fact_ar_invoice_lineitem4.invoice_currency,
                            fact_ar_invoice_lineitem4.tax_code,
                            fact_ar_invoice_lineitem4.tax_code1 as tax_code1,
                            fact_ar_invoice_lineitem4.gl_account,
                            fact_ar_invoice_lineitem4.gl_account_1,
                            fact_ar_invoice_lineitem4.baseline_date,
                            fact_ar_invoice_lineitem4.pay_term,
                            fact_ar_invoice_lineitem4.invoice_ref,
                            fact_ar_invoice_lineitem4.trading_partner,
                            fact_ar_invoice_lineitem4.due_date,
                            fact_ar_invoice_lineitem4.tax_registration_number,
                            fact_ar_invoice_lineitem4.dest_country,
                            fact_ar_invoice_lineitem4.reason_code,
                            fact_ar_invoice_lineitem4.invoice_status,
                            fact_ar_invoice_lineitem4.invoice_ref_1,
                            fact_ar_invoice_lineitem4.activity_center,
                            fact_ar_invoice_lineitem4.site,
                            fact_ar_invoice_lineitem4.payment_ref,
                            fact_ar_invoice_lineitem4.functional_area,
                            fact_ar_invoice_lineitem4.dr_cr_ind,
                            fact_ar_invoice_lineitem4.net_payment_terms_period,
                            fact_ar_invoice_lineitem4.days_1,
                            fact_ar_invoice_lineitem4.days_2,
                            fact_ar_invoice_lineitem4.src_system_name,
                            fact_ar_invoice_lineitem4.rank1,
                            fact_ar_invoice_lineitem3.amount_in_lc,
                            fact_ar_invoice_lineitem3.amount_in_gc,
                            fact_ar_invoice_lineitem3.lc_tax,
                            fact_ar_invoice_lineitem3.tax_original,
                            fact_ar_invoice_lineitem3.tax_amt,
                            fact_ar_invoice_lineitem3.tax_amt_tax_curr,
                            fact_ar_invoice_lineitem3.tax_original_in_curr1,
                            fact_ar_invoice_lineitem3.tax_original_in_curr2,
                            fact_ar_invoice_lineitem3.lc2_amount,
                            fact_ar_invoice_lineitem3.disc_amount

                        from

                            (
                                select
                                    fact_ar_invoice_lineitem2.billing_doc,
                                    fact_ar_invoice_lineitem2.document_number,
                                    fact_ar_invoice_lineitem2.src_system_name,
                                    sum(
                                        case
                                            when
                                                fact_ar_invoice_lineitem2.dr_cr_ind
                                                = 'H'
                                            then
                                                (
                                                    -1
                                                    * fact_ar_invoice_lineitem2.amount_in_lc
                                                )
                                            else fact_ar_invoice_lineitem2.amount_in_lc
                                        end
                                    ) as amount_in_lc,
                                    sum(
                                        case
                                            when
                                                fact_ar_invoice_lineitem2.dr_cr_ind
                                                = 'H'
                                            then
                                                (
                                                    -1
                                                    * fact_ar_invoice_lineitem2.amount_in_gc
                                                )
                                            else fact_ar_invoice_lineitem2.amount_in_gc
                                        end
                                    ) as amount_in_gc,
                                    sum(
                                        case
                                            when
                                                fact_ar_invoice_lineitem2.dr_cr_ind
                                                = 'H'
                                            then (-1 * fact_ar_invoice_lineitem2.lc_tax)
                                            else fact_ar_invoice_lineitem2.lc_tax
                                        end
                                    ) as lc_tax,
                                    sum(
                                        case
                                            when
                                                fact_ar_invoice_lineitem2.dr_cr_ind
                                                = 'H'
                                            then
                                                (
                                                    -1
                                                    * fact_ar_invoice_lineitem2.tax_original
                                                )
                                            else fact_ar_invoice_lineitem2.tax_original
                                        end
                                    ) as tax_original,
                                    sum(
                                        case
                                            when
                                                fact_ar_invoice_lineitem2.dr_cr_ind
                                                = 'H'
                                            then
                                                (-1 * fact_ar_invoice_lineitem2.tax_amt)
                                            else fact_ar_invoice_lineitem2.tax_amt
                                        end
                                    ) as tax_amt,
                                    sum(
                                        case
                                            when
                                                fact_ar_invoice_lineitem2.dr_cr_ind
                                                = 'H'
                                            then
                                                (
                                                    -1
                                                    * fact_ar_invoice_lineitem2.tax_amt_tax_curr
                                                )
                                            else
                                                fact_ar_invoice_lineitem2.tax_amt_tax_curr
                                        end
                                    ) as tax_amt_tax_curr,
                                    sum(
                                        case
                                            when
                                                fact_ar_invoice_lineitem2.dr_cr_ind
                                                = 'H'
                                            then
                                                (
                                                    -1
                                                    * fact_ar_invoice_lineitem2.tax_original_in_curr1
                                                )
                                            else
                                                fact_ar_invoice_lineitem2.tax_original_in_curr1
                                        end
                                    ) as tax_original_in_curr1,
                                    sum(
                                        case
                                            when
                                                fact_ar_invoice_lineitem2.dr_cr_ind
                                                = 'H'
                                            then
                                                (
                                                    -1
                                                    * fact_ar_invoice_lineitem2.tax_original_in_curr2
                                                )
                                            else
                                                fact_ar_invoice_lineitem2.tax_original_in_curr2
                                        end
                                    ) as tax_original_in_curr2,
                                    sum(
                                        case
                                            when
                                                fact_ar_invoice_lineitem2.dr_cr_ind
                                                = 'H'
                                            then
                                                (
                                                    -1
                                                    * fact_ar_invoice_lineitem2.lc2_amount
                                                )
                                            else fact_ar_invoice_lineitem2.lc2_amount
                                        end
                                    ) as lc2_amount,
                                    sum(
                                        case
                                            when
                                                fact_ar_invoice_lineitem2.dr_cr_ind
                                                = 'H'
                                            then
                                                (
                                                    -1
                                                    * fact_ar_invoice_lineitem2.disc_amount
                                                )
                                            else fact_ar_invoice_lineitem2.disc_amount
                                        end
                                    ) as disc_amount

                                -- --Debit and Credit indicator(dr_cr_ind) where Debit
                                -- =S and Credit =H
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

                        -- fact_ar_invoice_lineitem3 gives us the sum of amount based
                        -- on combination of document number and billing doc.
                        inner join

                            (
                                select

                                    fact_ar_invoice_lineitem1.legal_entity
                                    as legal_entity,
                                    fact_ar_invoice_lineitem1.customer as customer,
                                    fact_ar_invoice_lineitem1.indicator_spl_gl
                                    as indicator_spl_gl,
                                    fact_ar_invoice_lineitem1.clearing_date
                                    as clearing_date,
                                    fact_ar_invoice_lineitem1.receipt_number
                                    as receipt_number,
                                    fact_ar_invoice_lineitem1.fiscal_year as fiscal_year

                                    ,
                                    fact_ar_invoice_lineitem1.document_number
                                    as document_number,
                                    fact_ar_invoice_lineitem1.billing_doc as billing_doc

                                    ,
                                    fact_ar_invoice_lineitem1.posting_date
                                    as posting_date,
                                    fact_ar_invoice_lineitem1.trx_date as document_date,
                                    fact_ar_invoice_lineitem1.entered_on as entered_on,
                                    fact_ar_invoice_lineitem1.invoice_currency
                                    as invoice_currency,
                                    fact_ar_invoice_lineitem1.tax_code as tax_code,
                                    fact_ar_invoice_lineitem1.tax_code1_s as tax_code1,
                                    fact_ar_invoice_lineitem1.natural_account
                                    as gl_account,
                                    fact_ar_invoice_lineitem1.natural_account_1
                                    as gl_account_1,
                                    fact_ar_invoice_lineitem1.baseline_date
                                    as baseline_date,
                                    fact_ar_invoice_lineitem1.pay_term as pay_term,
                                    fact_ar_invoice_lineitem1.invoice_ref as invoice_ref

                                    ,
                                    fact_ar_invoice_lineitem1.trading_partner
                                    as trading_partner,
                                    fact_ar_invoice_lineitem1.due_date as due_date,
                                    fact_ar_invoice_lineitem1.tax_registration_number
                                    as tax_registration_number,
                                    fact_ar_invoice_lineitem1.dest_country
                                    as dest_country,
                                    fact_ar_invoice_lineitem1.reason_code as reason_code

                                    ,
                                    fact_ar_invoice_lineitem1.invoice_status
                                    as invoice_status,
                                    fact_ar_invoice_lineitem1.invoice_ref_1
                                    as invoice_ref_1,
                                    fact_ar_invoice_lineitem1.activity_center
                                    as activity_center,
                                    fact_ar_invoice_lineitem1.site as site,
                                    fact_ar_invoice_lineitem1.payment_ref as payment_ref

                                    ,
                                    fact_ar_invoice_lineitem1.functional_area
                                    as functional_area,
                                    fact_ar_invoice_lineitem1.dr_cr_ind as dr_cr_ind,
                                    fact_ar_invoice_lineitem1.document_line_number
                                    as document_line_number,
                                    fact_ar_invoice_lineitem1.net_payment_terms_period,
                                    fact_ar_invoice_lineitem1.days_1,
                                    fact_ar_invoice_lineitem1.days_2,
                                    fact_ar_invoice_lineitem1.src_system_name,
                                    row_number() over (
                                        partition by billing_doc, document_number
                                        order by
                                            legal_entity,
                                            customer,
                                            indicator_spl_gl,
                                            clearing_date,
                                            receipt_number,
                                            fiscal_year,
                                            document_number,
                                            billing_doc,
                                            posting_date,
                                            trx_date,
                                            entered_on,
                                            invoice_currency,
                                            tax_code,
                                            tax_code1_s,
                                            natural_account,
                                            natural_account_1,
                                            baseline_date,
                                            pay_term,
                                            invoice_ref,
                                            trading_partner,
                                            due_date,
                                            tax_registration_number,
                                            dest_country,
                                            reason_code,
                                            invoice_status,
                                            invoice_ref_1,
                                            activity_center,
                                            site,
                                            payment_ref,
                                            functional_area,
                                            dr_cr_ind,
                                            document_line_number,
                                            net_payment_terms_period,
                                            days_1,
                                            days_2 desc
                                    ) rank1

                                from
                                    live.fact_ar_invoice_lineitem fact_ar_invoice_lineitem1

                                where
                                    ifnull(fact_ar_invoice_lineitem1.slt_delete, '')
                                    <> 'X'
                                    and fact_ar_invoice_lineitem1.src_system_name
                                    = 'sap'
                            ) fact_ar_invoice_lineitem4

                            -- fact_ar_invoice_lineitem4 gives the other deatils for
                            -- descripitive fields based on billing doc and document
                            -- number. In case of multiple line items it selects the
                            -- first row based on row number and rank function.
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

                -- and  fact_ar_invoice_lineitem.src_system_name=
                -- FACT_ACCOUNTING_DOCUMENT_HEADER_sap.src_system_name_s
                -- and ifnull(FACT_ACCOUNTING_DOCUMENT_HEADER_sap.slt_delete_s,'')<>'X'
                where

                    -- fact_AR_INVOICE_BILLING_ITEMS.src_system_name='sap'
                    ifnull(fact_ar_invoice_billing_items.slt_delete, '') <> 'X'

            ) fact

        -- Base table in SAP AR is combination of dwh.fact_ar_invoice_lineitem and
        -- dwh.[vw_fact_ar_invoice_billing_items]. From dwh.fact_ar_invoice_lineitem
        -- we are getting information for documents where invoice_type not in
        -- ('31','32','33','34','35','36','NA','RV','SD','ZC'). for these invoice
        -- types Data is coming from dwh.[vw_fact_ar_invoice_billing_items].
        -- (Invocie type should not be '31'GUI Cust_3 copies,'32'GUI Cust_2
        -- copies,'33'GUI Cust_3 copies CN,'34'GUI Cust_2 copies CN,'35'E-GUI Cust_3
        -- copies,'36'GUI Cust_ZeroTax Frg,'NA'Adjustment Notes,'RV'  Billing
        -- doc.transfer,'SD'Support Documents,'ZC'RO local billing doc)
        left join
            (
                select
                    a.fiscal_year,
                    a.legal_entity,
                    a.document_number,
                    min(document_line_number) as min_document_line_number
                from
                    (

                        select
                            f1.fiscal_year,
                            f1.legal_entity,
                            f1.document_number,
                            f1.document_line_number

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

            -- Invocie type should not be '31'GUI Cust_3 copies,'32'GUI Cust_2
            -- copies,'33'GUI Cust_3 copies CN,'34'GUI Cust_2 copies CN,'35'E-GUI
            -- Cust_3 copies,'36'GUI Cust_ZeroTax Frg,'NA'Adjustment Notes,'RV'
            -- Billing doc.transfer,'SD'Support Documents,'ZC'RO local billing doc
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
                        select
                            fiscal_year,
                            le_number,
                            document_number,
                            sum(
                                case
                                    when debit_credit_ind = 'H'
                                    then (-1 * lc_amount)
                                    else lc_amount
                                end
                            ) as discount_taken_unearned
                        from live.fact_accounting_document_line_items

                        where
                            account_type = 'S'
                            and general_ledger_account = '0004010015'  -- If line item is S= G/L line and  G/L account is 0004010015 which is for Unearned Cash Disc Allowed

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
            and

            fact.site = xxetn_map_unit_sap_ora.sap_profit_center

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
                            acct_type_s = 'D' and

                            ifnull(slt_delete, '') <> 'X' and active_flag_s = 'Y'

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
            end between (
                case
                    when (date_type = 'D' and ifnull(min_day_limit_s, 0) = 0)
                    then 0

                    when (date_type = 'B' and ifnull(min_day_limit_s, 0) = 0)
                    then 0

                    when (date_type = 'C' and ifnull(min_day_limit_s, 0) = 0)
                    then 0
                    else min_day_limit_s
                end
            ) and

            (
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
