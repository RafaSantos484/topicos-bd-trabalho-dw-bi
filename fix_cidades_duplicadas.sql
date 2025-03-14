/*
A mesma cidade foi cadastrada duas vezes (Salvador e SSA, Rio de Janeiro e RJ e São Paulo e SP);
*/

-- Alterar FK de clientes para id_cidade correto
UPDATE c##dw_oltp.cliente
SET id_cidade = (SELECT c2.id_cidade FROM c##dw_oltp.cidade c2 WHERE c2.descricao = 'Salvador')
WHERE id_cidade = (SELECT c1.id_cidade FROM c##dw_oltp.cidade c1 WHERE c1.descricao = 'SSA');

UPDATE c##dw_oltp.cliente
SET id_cidade = (SELECT c2.id_cidade FROM c##dw_oltp.cidade c2 WHERE c2.descricao = 'Rio de Janeiro')
WHERE id_cidade = (SELECT c1.id_cidade FROM c##dw_oltp.cidade c1 WHERE c1.descricao = 'RJ');

UPDATE c##dw_oltp.cliente
SET id_cidade = (SELECT c2.id_cidade FROM c##dw_oltp.cidade c2 WHERE c2.descricao = 'São Paulo')
WHERE id_cidade = (SELECT c1.id_cidade FROM c##dw_oltp.cidade c1 WHERE c1.descricao = 'SP');

-- Remover cidades inválidas
DELETE FROM c##dw_oltp.cidade WHERE descricao IN ('SSA', 'RJ', 'SP');

-- COMMIT;
