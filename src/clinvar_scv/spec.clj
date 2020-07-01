(ns clinvar-scv.spec
  (:require [clojure.spec.alpha :as spec]
            [clinvar-scv.spec.clinical-assertion :as clinical-assertion]
            [clinvar-scv.spec.clinical-assertion-observation :as clinical-assertion-observation]
            [clinvar-scv.spec.clinical-assertion-trait :as clinical-assertion-trait]
            [clinvar-scv.spec.clinical-assertion-trait-set :as clinical-assertion-trait-set]
            [clinvar-scv.spec.clinical-assertion-variation :as clinical-assertion-variation]
            [clinvar-scv.spec.gene :as gene]
            [clinvar-scv.spec.gene-association :as gene-association]
            [clinvar-scv.spec.rcv-accession :as rcv-accession]
            [clinvar-scv.spec.submission :as submission]
            [clinvar-scv.spec.submitter :as submitter]
            [clinvar-scv.spec.trait :as trait]
            [clinvar-scv.spec.trait-mapping :as trait-mapping]
            [clinvar-scv.spec.trait-set :as trait-set]
            [clinvar-scv.spec.variation :as variation]
            [clinvar-scv.spec.variation-archive :as variation-archive]

            ;[clinvar-scv.spec :refer :all]
            ;[clinvar-scv.spec.gene :as gene]
            [clojure.string :as s]
            ))

(defn validate [event]
  ;(println event)
  (let [entity-type (-> event :data :content :entity-type)]
    (merge event
           (spec/explain-data (keyword (str "clinvar-scv.spec." (s/replace entity-type "_" "-"))  (s/replace entity-type "_" "-"))
                              (-> event :data :content)))
    ;(println content)
    ;content
    ; (merge event (spec/explain-data ::clinical-assertion/clinical-assertion (-> event :data :content))))
  ))
