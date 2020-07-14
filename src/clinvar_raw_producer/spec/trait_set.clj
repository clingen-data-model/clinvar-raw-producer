(ns clinvar-raw-producer.spec.trait-set
  (:require [clojure.spec.alpha :as spec]
            [clinvar-raw-producer.spec.spec-asserts :as sa]))

(spec/def ::id sa/string-is-int?)
(spec/def ::trait_ids not-empty)
(spec/def ::type not-empty) ; TODO looks like an enumeration

; Optional
(spec/def ::content not-empty)

(spec/def ::trait-set
  (spec/keys :req-un [::id
                      ::trait_ids
                      ::type]
             :opt-un [::content
                      ]))
