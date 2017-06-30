(ns storm-start.word-count
  (:import [org.apache.storm StormSubmitter LocalCluster])
  (:use [org.apache.storm clojure])
  (:require [clojure.java.io :as io]
            [org.apache.storm.config :as config])
  (:gen-class))

(defspout sentence-spout ["sentence"]
  [conf context collector]
  (let [sentences ["a little brown dog"
                   "the man petted the dog"
                   "four score and seven years ago"
                   "an apple a day keeps the doctor away"]]
    (spout
     (nextTuple []
       (Thread/sleep 100)
       (emit-spout! collector [(rand-nth sentences)])         
       )
     (ack [id]
        ;; You only need to define this method for reliable spouts
        ;; (such as one that reads off of a queue like Kestrel)
        ;; This is an unreliable spout, so it does nothing here
        ))))

(defspout sentence-spout-parameterized ["word"] {:params [sentences] :prepare false}
  [collector]
  (Thread/sleep 500)
  (emit-spout! collector [(rand-nth sentences)]))

(defbolt split-sentence ["word"] [tuple collector]
  (let [words (.split (.getString tuple 0) " ")]
    (doseq [w words]
      (emit-bolt! collector [w] :anchor tuple))
    (ack! collector tuple)
    ))

(defbolt word-count ["word" "count"] {:prepare true}
  [conf context collector]
  (let [counts (atom {})]
    (bolt
     (execute [tuple]
       (let [word (.getString tuple 0)]
         (swap! counts (partial merge-with +) {word 1})
         (emit-bolt! collector [word (@counts word)] :anchor tuple)
         (ack! collector tuple)
         )))))


(defbolt output-sink ["word"] {:prepare true}
  [conf context collector]
  (let [w (io/writer "/tmp/storm-sink.out" :append true)]
    (bolt
      (execute [tuple]
        (.write w (str tuple "\n"))
        (.flush w)
        (ack! collector tuple)        
        ))))


; (undef make-topology)
(defmulti make-topology identity)

(defmethod make-topology :word-count [_]
  (topology
   {"sentence-spout" (spout-spec sentence-spout)
    "sentence-spout-parameterized" (spout-spec (sentence-spout-parameterized
                     ["the cat jumped over the door"
                      "greetings from a faraway land"])
                     :p 2)}
   {"split" (bolt-spec {"sentence-spout" :shuffle "sentence-spout-parameterized" :shuffle}
                   split-sentence
                   :p 5)
    "word-count" (bolt-spec {"split" ["word"]}
                   word-count
                   :p 6)
    "sink" (bolt-spec {"word-count" :shuffle}
                      output-sink
                      :p 1)
    }
   
   ))

(defn run-local! [topology]
  (let [cluster (LocalCluster.)]
    (.submitTopology cluster "word-count" {config/TOPOLOGY-DEBUG true} topology)
    (Thread/sleep 10000)
    (.shutdown cluster)
    ))

(defn submit-topology! [name topology]
  (StormSubmitter/submitTopology
   name
   {config/TOPOLOGY-DEBUG true
    config/TOPOLOGY-WORKERS 3}
   topology))

(defn -main
  ([]
   (run-local! (make-topology :word-count)))
  ([name]
   (submit-topology! name (make-topology :word-count))))
