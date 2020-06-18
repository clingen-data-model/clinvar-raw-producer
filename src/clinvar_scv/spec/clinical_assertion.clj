(ns clinvar-scv.spec.clinical-assertion
  (:require [clojure.spec.alpha :as spec]))

(spec/def ::id #(re-matches #"SCV\d+" %))

(spec/def ::clinical-assertion
  (spec/keys :req-un [::variation_id
                      ::variation_archive_id
                      ::submitter_id
                      ::date_last_updated
                      ::interpretation_comments
                      ::interpretation_description
                      ::trait_set_id
                      ::internal_id
                      ::type
                      ::submission_id
                      ::local_key
                      ::clinical_assertion_observation_ids
                      ::title
                      ::assertion_type
                      ::rcv_accession_id
                      ::clinical_assertion_trait_set_id
                      ::id
                      ::submission_names
                      ::record_status
                      ::date_created
                      ::review_status
                      ::interpretation_date_last_evaluated
                      ::version]))
