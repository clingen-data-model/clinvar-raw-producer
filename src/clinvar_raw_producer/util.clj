(ns clinvar-raw-producer.util
  (:gen-class))

(defn in?
  "Returns true if e in col"
  [e col]
  (some #(= e %) col))

(defn not-in? [e col]
  (not (in? e col)))

(defn match-in?
  "Returns true if any e in col matches pattern"
  [pattern col]
  (some? (some #(re-find (re-pattern pattern) %) col)))

(defn match-not-in? [pattern col]
  (not (match-in? pattern col)))

(defn match-every?
  "Returns true if all e in col matches pattern"
  [pattern col]
  (every? #(re-find (re-pattern pattern) %) col))

(defn unordered-eq?
  "Returns true if the two collections are equal, regardless of order"
  [col1 col2]
  (= (sort col1) (sort col2)))
