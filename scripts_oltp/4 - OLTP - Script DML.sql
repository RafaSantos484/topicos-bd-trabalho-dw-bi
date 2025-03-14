-- Insers�o do carrinho de compras 
DECLARE
  v_ano int;
  v_id_aparelho aparelho.ID_Aparelho%TYPE;
  v_quantidade int;
  v_valor NUMBER;
  v_qtde_itens_diferentes int;
BEGIN
  FOR i IN 1..10000 LOOP   -- Loop para as 10.000 compras realizadas
    -- Recupera o ano da compra
   SELECT extract(year from Data) INTO v_ano FROM COMPRA WHERE ID_Compra = i;

    v_qtde_itens_diferentes := 1;
    IF ( MOD(i,179) = 0 ) THEN -- A cada 179 compras, haverá 2 itens diferentes comprados no carrinho;
       v_qtde_itens_diferentes := 2;
    END IF;

    FOR j IN 1..v_qtde_itens_diferentes LOOP	
        
		-- Seleciona um ID_Aparelho aleatÃ³rio existente na tabela Aparelhos
		SELECT ID_Aparelho INTO v_id_aparelho
		FROM (SELECT ID_Aparelho FROM aparelho WHERE ID_Aparelho NOT IN (SELECT ID_Aparelho FROM Carrinho) )
		--ORDER BY DBMS_RANDOM.VALUE) 
		WHERE ROWNUM = 1;
		        
        -- Gera uma quantidade aleatÃ³ria entre 1 e 3
		v_quantidade := FLOOR(DBMS_RANDOM.VALUE(1, 4));
		
		-- Gera um valor aleatÃ³rio entre 900.00 e 12000.00, multiplica pela quantidade de itens comprados
        -- E reajusta o valor a cada ano da compra (5%) após o 1o ano que foi 2020.
		v_valor := DBMS_RANDOM.VALUE(900.00, 12000.00) * v_quantidade;
        IF (v_ano > 2020 ) THEN
            v_valor := v_valor + ( v_valor * ( (v_ano - 2020) * 5) / 100 );
		END IF;
       
		-- Insere o item de compra na tabela Carrinho
		INSERT INTO carrinho (ID_Carrinho, ID_Compra, ID_Aparelho, Quantidade, Valor)
		VALUES (SQ_Carrinho.NEXTVAL, i, v_id_aparelho, v_quantidade, v_valor);
        
    END LOOP;
  END LOOP;
  
  COMMIT;
END;




/*
DECLARE
  v_id_compra compra.ID_Compra%TYPE;
  v_id_aparelho aparelho.ID_Aparelho%TYPE;
  v_quantidade NUMBER;
  v_valor NUMBER;
BEGIN
  FOR i IN 1..10000 LOOP
    -- Seleciona um ID_Compra aleatÃ³rio existente na tabela Compras
    SELECT ID_Compra INTO v_id_compra
    FROM (SELECT ID_Compra FROM compra ORDER BY DBMS_RANDOM.VALUE) 
    WHERE ROWNUM = 1;
    
    -- Seleciona um ID_Aparelho aleatÃ³rio existente na tabela Aparelhos
    SELECT ID_Aparelho INTO v_id_aparelho
    FROM (SELECT ID_Aparelho FROM aparelho ORDER BY DBMS_RANDOM.VALUE) 
    WHERE ROWNUM = 1;
    
    -- Gera uma quantidade aleatÃ³ria entre 1 e 3
    v_quantidade := FLOOR(DBMS_RANDOM.VALUE(1, 4));
    
    -- Gera um valor aleatÃ³rio entre 900.00 e 12000.00
    v_valor := DBMS_RANDOM.VALUE(900.00, 12000.00);
    
    -- Insere o item de compra na tabela Carrinho
    INSERT INTO carrinho (ID_Carrinho, ID_Compra, ID_Aparelho, Quantidade, Valor)
    VALUES (SQ_Carrinho.NEXTVAL, v_id_compra, v_id_aparelho, v_quantidade, v_valor);
  END LOOP;
  
  COMMIT;
END;
*/
