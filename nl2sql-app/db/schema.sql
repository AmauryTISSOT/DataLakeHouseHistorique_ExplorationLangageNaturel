-- ============================================================================
-- Schema en etoile — banc d'essai text-to-SQL (Coupe du Monde)
-- Dialecte : SQLite.
-- NB : ce fichier est AUSSI la source de verite injectee dans le prompt du LLM
--      (d'ou les commentaires explicites sur la dimension a roles).
-- ============================================================================

-- Dimension : editions de la Coupe du Monde (une ligne par annee / pays hote).
CREATE TABLE DIM_EDITION (
    edition_id  INTEGER PRIMARY KEY,    -- cle de substitution
    annee       INTEGER NOT NULL,       -- ex. 2018, 2022
    pays_hote   TEXT    NOT NULL         -- ex. 'Russia', 'Qatar'
);

-- Dimension : equipes nationales.
CREATE TABLE DIM_EQUIPE (
    equipe_id   INTEGER PRIMARY KEY,    -- cle de substitution
    nom_equipe  TEXT    NOT NULL UNIQUE  -- ex. 'Argentina', 'France'
);

-- Fait : un match par ligne. Grain = un match.
-- DIM_EQUIPE est une DIMENSION A ROLES : chaque match la reference DEUX fois.
--   equipe_domicile_id  : equipe qui recoit  (jouant "a domicile").
--   equipe_exterieur_id : equipe qui se deplace ("a l'exterieur").
-- IMPORTANT pour toute agregation par equipe :
--   Une equipe joue tantot a domicile, tantot a l'exterieur. Pour calculer les
--   buts marques (ou encaisses, ou le nombre de matchs) d'UNE equipe, il faut
--   AGREGER SES DEUX ROLES : additionner les cas ou elle est equipe_domicile_id
--   (ses buts = score_domicile) ET les cas ou elle est equipe_exterieur_id
--   (ses buts = score_exterieur). Un simple JOIN sur un seul role sous-compte.
CREATE TABLE FAIT_MATCH (
    match_id            INTEGER PRIMARY KEY,   -- cle de substitution
    edition_id          INTEGER NOT NULL REFERENCES DIM_EDITION(edition_id),
    equipe_domicile_id  INTEGER NOT NULL REFERENCES DIM_EQUIPE(equipe_id),
    equipe_exterieur_id INTEGER NOT NULL REFERENCES DIM_EQUIPE(equipe_id),
    score_domicile      INTEGER NOT NULL,      -- buts de l'equipe a domicile
    score_exterieur     INTEGER NOT NULL,      -- buts de l'equipe a l'exterieur
    phase               TEXT    NOT NULL,      -- ex. 'Group stage', 'Final'
    date_match          TEXT,                  -- 'YYYY-MM-DD'
    stade               TEXT                   -- nom du stade (attribut degenere)
);
