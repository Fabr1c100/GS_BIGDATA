%sql -- 06 | Consulta de Relato via app (MVP)
SELECT * FROM luz_segura.bronze_relatos WHERE id_relato = 'APP-20261116192615-9553';
SELECT * FROM luz_segura.prata_relatos  WHERE id_relato = 'APP-20261116192615-9553';
SELECT * FROM luz_segura.fato_ocorrencia WHERE id_origem = 'APP-20261116192615-9553';