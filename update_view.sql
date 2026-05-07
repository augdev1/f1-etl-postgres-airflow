DROP VIEW IF EXISTS vw_f1_results_powerbi;

CREATE VIEW vw_f1_results_powerbi AS
SELECT
    id,
    temporada,
    rodada,
    grande_premio AS corrida,
    data_corrida,
    posicao_final,
    pontuacao AS pontos,
    posicao_largada,
    piloto_id,
    piloto,
    escuderia AS equipe
FROM (
    SELECT
        id,
        payload->>'season' AS temporada,
        payload->>'round' AS rodada,
        payload->>'raceName' AS grande_premio,
        payload->>'date' AS data_corrida,
        (payload->'Results'->>'position')::int AS posicao_final,
        (payload->'Results'->>'points')::float AS pontuacao,
        (payload->'Results'->>'grid')::int AS posicao_largada,
        payload->'Results'->'Driver'->>'driverId' AS piloto_id,
        (
            payload->'Results'->'Driver'->>'givenName'
            || ' '
            || payload->'Results'->'Driver'->>'familyName'
        ) AS piloto,
        payload->'Results'->'Constructor'->>'name' AS escuderia
    FROM f1_results
) sub;
