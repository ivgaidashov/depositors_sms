-------------------------------------------------------------------------
/*Возвращает сохраненный мобильный номер телефона клиента*/

FUNCTION f_mobile_phone(p_icusnum NUMBER) RETURN VARCHAR2 AS 
  v_return_phn VARCHAR2(15) := 'error';
         
  CURSOR get_phones IS
         select case /*1*/ when regexp_like(replace(c.ph_city, ' ', ''), '[9]{1}[0-9]{2}') and length(replace(c.ph_city, ' ', ''))=3 /*e.g. 920*/ and regexp_like(replace(c.ph_num, ' ', ''), '[0-9]{7}')  and length((replace(c.ph_num, ' ', '')))=7   /*e.g. 1112233*/ then nvl(replace(c.ph_cnt, ' ', ''), '7')||(replace(c.ph_city, ' ', ''))||(replace(c.ph_num, ' ', ''))
              /*2*/ when regexp_like(replace(c.ph_num, ' ', ''), '[9]{1}[0-9]{9}') and length(replace(c.ph_num, ' ', ''))=10 /*e.g. 9201112233*/ then to_char(nvl(c.PH_CNT, '7')||replace(c.ph_num, ' ', ''))
              /*3*/ when regexp_like(replace(c.ph_city, ' ', ''), '[89][0-9]{2}') /*e.g. 8920*/ and length(replace(c.ph_city, ' ', ''))=4 and regexp_like(replace(c.ph_num, ' ', ''), '[0-9]{7}') and length(replace(c.ph_num, ' ', ''))=7 /*e.g. 1112233*/ then '7'||substr(to_char(c.ph_city), -3)||replace(c.ph_num, ' ', '')
              /*4*/ when regexp_like(replace(c.ph_num, ' ', ''), '[9][0-9]{9,9}') and length(replace(c.ph_num, ' ', ''))=11/*e.g. 89201112233*/ then '7'||substr(replace(c.ph_num, ' ', ''), -10)   
              /*5*/ when regexp_like(c.PH_CNT||c.PH_CITY||c.PH_NUMNUM, '((7){1})((9){1})(\d{9})') and c.ph_type = 4 then c.PH_CNT||c.PH_CITY||c.PH_NUMNUM
          else 'error' end "phone_number"
        from cus_phone c
        where c.icusnum = p_icusnum;
        
  BEGIN
    FOR phn IN get_phones LOOP
      IF phn."phone_number" <> 'error' THEN
        v_return_phn := phn."phone_number";
      END IF;
    END LOOP;
    
    RETURN v_return_phn;
 END;
 
-------------------------------------------------------------------------
/*Возвращает список вкладчиков, чьи договора заканчиваются через 5, 6, 7, 8, 9 или 10 для СМС информирования*/

 FUNCTION f_depositors_sms RETURN t_dep_sms_tab PIPELINED  IS
  l_row_as_object t_dep_sms_row := t_dep_sms_row (NULL, NULL, NULL, NULL, NULL, NULL);
	
  CURSOR get_depositors_to_notify IS
         SELECT iqdgcli,
                IQDGIDENT,
                iqdgnum, 
                dqdgbeg,
                dqdgend            
         FROM QDG_MF q 
         WHERE q.iqdgstatus = 3 /*статус Исполняемый*/
               AND q.IQDGMAK not in (12,13,14 /*До Востребования*/, 96, 101 /*Юр. лица*/)
               AND dqdgend = (select min(dqdgend) from QDG_MF where dqdgend between DjEnv.Get_LsDate+6 and DjEnv.Get_LsDate+10)
               /*старый вариант case when exists (select 1 from QDG_MF where dqdgend =DjEnv.Get_LsDate+6) then DjEnv.Get_LsDate+6
                                  when exists (select 1 from QDG_MF where dqdgend =DjEnv.Get_LsDate+7) then DjEnv.Get_LsDate+7
                                  when exists (select 1 from QDG_MF where dqdgend =DjEnv.Get_LsDate+8) then DjEnv.Get_LsDate+8
                                  when exists (select 1 from QDG_MF where dqdgend =DjEnv.Get_LsDate+9) then DjEnv.Get_LsDate+9
                                  when exists (select 1 from QDG_MF where dqdgend =DjEnv.Get_LsDate+10) then DjEnv.Get_LsDate+10
                               end */
               AND NOT EXISTS (SELECT 1 FROM gis_dep_sms g WHERE g.iqdgident = q.iQdgIdent);
 
  BEGIN
  FOR depositor IN get_depositors_to_notify LOOP

     l_row_as_object.iqdgcli   := depositor.iqdgcli;
     l_row_as_object.IQDGIDENT := depositor.IQDGIDENT;
     l_row_as_object.iqdgnum   := depositor.iqdgnum;
     l_row_as_object.dqdgend   := depositor.dqdgend;
     l_row_as_object.cmessage  := 'Срок договора №'||depositor.iqdgnum||' от '||to_char(depositor.dqdgbeg,'dd.mm.yyyy')||' заканчивается '||to_char(depositor.dqdgend,'dd.mm.yyyy');
     l_row_as_object.cphn_nmb  := f_mobile_phone(depositor.iqdgcli);
     PIPE ROW (l_row_as_object);
 END LOOP;

 RETURN;
 END;    
 