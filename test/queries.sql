-- :name add-fruit! :! :n
INSERT INTO fruits
(name, appearance, cost, grade)
VALUES
(:name, :appearance, :cost, :grade)

-- :name get-fruit :? :*
SELECT * FROM fruits
WHERE name = :name
