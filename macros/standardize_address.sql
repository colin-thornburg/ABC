{% macro standardize_address(address_field) %}

  -- Convert the address field to lowercase
  LOWER({{ address_field }}) AS standardized_address,

  -- Replace 'St' or 'st' with 'street'
  REGEXP_REPLACE(standardized_address, '\\bst\\b|\\bSt\\b', 'street') AS standardized_address_st,

  -- Replace 'St.' or 'st.' with 'street'
  REPLACE(standardized_address_st, 'st.', 'street') AS standardized_address_st_dot,
  REPLACE(standardized_address_st_dot, 'street.', 'street') AS standardized_address_st_dot_no_period,

  -- Replace 'Ave' or 'ave' with 'avenue'
  REGEXP_REPLACE(standardized_address_st_dot_no_period, '\\bave\\b|\\bAve\\b', 'avenue') AS standardized_address_ave,

  -- Replace 'Ave.' or 'ave.' with 'avenue'
  REPLACE(standardized_address_ave, 'ave.', 'avenue') AS standardized_address_ave_dot,
  REPLACE(standardized_address_ave_dot, 'avenue.', 'avenue') AS standardized_address_ave_final,

  -- Replace 'Dr' or 'dr' with 'drive'
  REGEXP_REPLACE(standardized_address_ave_final, '\\bdr\\b|\\bDr\\b', 'drive') AS standardized_address_dr,

  -- Replace 'Dr.' or 'dr.' with 'drive'
  REPLACE(standardized_address_dr, 'dr.', 'drive') AS standardized_address_dr_dot,
  REPLACE(standardized_address_dr_dot, 'drive.', 'drive') AS standardized_address_dr_final,
  
  -- Replace 'Ln' or 'ln' with 'lane'
  REGEXP_REPLACE(standardized_address_dr_final, '\\bln\\b|\\bLn\\b', 'lane') AS standardized_address_ln,

  -- Replace 'Ln.' or 'ln.' with 'lane'
  REPLACE(standardized_address_ln, 'ln.', 'lane') AS standardized_address_ln_dot,
  REPLACE(standardized_address_ln_dot, 'lane.', 'lane') AS standardized_address_final

{% endmacro %}
