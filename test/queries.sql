-- :name add-fruit! :! :n
INSERT INTO fruits
(name, appearance, cost, grade)
VALUES
(:name, :appearance, :cost, :grade)

-- :name get-fruit :? :*
-- :doc gets fruit by name
SELECT * FROM fruits
WHERE name = :name
