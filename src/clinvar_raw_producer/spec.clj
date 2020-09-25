(ns clinvar-raw-producer.spec
  (:require [clojure.spec.alpha :as spec]
            [clinvar-raw-producer.spec.clinical-assertion :as clinical-assertion]
            [clinvar-raw-producer.spec.clinical-assertion-observation :as clinical-assertion-observation]
            [clinvar-raw-producer.spec.clinical-assertion-trait :as clinical-assertion-trait]
            [clinvar-raw-producer.spec.clinical-assertion-trait-set :as clinical-assertion-trait-set]
            [clinvar-raw-producer.spec.clinical-assertion-variation :as clinical-assertion-variation]
            [clinvar-raw-producer.spec.gene :as gene]
            [clinvar-raw-producer.spec.gene-association :as gene-association]
            [clinvar-raw-producer.spec.rcv-accession :as rcv-accession]
            [clinvar-raw-producer.spec.submission :as submission]
            [clinvar-raw-producer.spec.submitter :as submitter]
            [clinvar-raw-producer.spec.trait :as trait]
            [clinvar-raw-producer.spec.trait-mapping :as trait-mapping]
            [clinvar-raw-producer.spec.trait-set :as trait-set]
            [clinvar-raw-producer.spec.variation :as variation]
            [clinvar-raw-producer.spec.variation-archive :as variation-archive]

            [taoensso.timbre :as timbre
             :refer [log trace debug info warn error fatal report
                     logf tracef debugf infof warnf errorf fatalf reportf
                     spy get-env]]
            [clojure.string :as s]
            ))

(defn validate [event]
  (let [entity-type (-> event :data :content :entity_type)]
    (merge event
           (spec/explain-data (keyword (str "clinvar-raw-producer.spec." (s/replace entity-type "_" "-"))  (s/replace entity-type "_" "-"))
                              (-> event :data :content)))))
