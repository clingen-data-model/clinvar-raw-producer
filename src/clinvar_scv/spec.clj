(ns clinvar-scv.spec
  (:require [clojure.spec.alpha :as spec]
            [clinvar-scv.spec.clinical-assertion :as clinical-assertion]))

(defn validate [event]
  (merge event (spec/explain-data ::clinical-assertion/clinical-assertion (-> event :data :content))))
