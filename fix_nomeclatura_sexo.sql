/*
Alguns clientes possuem nomenclatura diferente para o atributo sexo. Ao invés de ‘M’, aparece 2 e ao invés de ‘F’ aparece 1
*/

UPDATE c##dw_oltp.cliente
SET sexo = 'M'
WHERE sexo = '2';

UPDATE c##dw_oltp.cliente
SET sexo = 'F'
WHERE sexo = '1';

-- COMMIT;
