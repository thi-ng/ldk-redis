(ns thi.ng.ldk.store.redis
  (:require
   [thi.ng.ldk.core.api :as api :refer [as-node]]
   [taoensso.carmine :as red]
   [clojure.pprint :refer [pprint]]))

(def redis-conn {:pool {} :spec {}})

(defmacro rexec [conn & body] `(red/wcar ~conn ~@body))

(defmacro get-indexed
  [conn idx x] `(as-node (rexec ~conn (red/hget ~idx ~x))))

(defmacro get-many [conn idx coll] `(rexec ~conn (apply red/hmget ~idx ~coll)))

(defmacro get-set [conn key] `(rexec ~conn (red/smembers ~key)))

(def ^:dynamic *hashimpl* (comp str hash api/index-value))

(defn index-triple
  [conn s p o]
  (let [sh (*hashimpl* s)
        ph (*hashimpl* p)
        oh (*hashimpl* o)]
    (rexec conn
           (red/hsetnx "subj" sh s)
           (red/hsetnx "pred" ph p)
           (red/hsetnx "obj" oh o))
    [sh ph oh]))

(defn clear-store
  [conn]
  (rexec conn (red/flushdb)))

(defn add*
  [conn s p o]
  (let [[sh ph oh :as t] (index-triple conn s p o)]
    (rexec conn
           (red/sadd (str "sp" sh) ph)
           (red/sadd (str "po" ph) oh)
           (red/sadd (str "op" oh) ph)
           (red/sadd (str "spo" sh ph) oh)
           (red/sadd (str "pos" ph oh) sh)
           (red/sadd (str "ops" oh ph) sh))
    t))

(defn triples-sp
  [conn s p coll]
  (let [s (as-node s) p (as-node p)]
    (map (fn [o] [s p (as-node o)]) (get-many conn "obj" coll))))

(defn triples-po
  [conn p o coll]
  (let [p (as-node p) o (as-node o)]
    (map (fn [s] [(as-node s) p o]) (get-many conn "subj" coll))))

(defn triples-op
  [conn o p coll]
  (let [o (as-node o) p (as-node p)]
    (map (fn [s] [(as-node s) p o]) (get-many conn "subj" coll))))

(defn reduce-triples
  [conn f base-val base-hash inner-idx coll-idx coll]
  (mapcat
   (fn [[c inner]] (f conn base-val c inner))
   (zipmap (get-many conn coll-idx coll)
           (map #(get-set conn (str inner-idx base-hash %)) coll))))

(defrecord RedisStore [conn]
  api/PModel
  (add-statement [this s p o]
    (add* conn s p o) this)
  (subject? [this x]
    (get-indexed conn "subj" (*hashimpl* x)))
  (predicate? [this x]
    (get-indexed conn "pred" (*hashimpl* x)))
  (object? [this x]
    (get-indexed conn "obj" (*hashimpl* x)))
  (select [this s p o]
    (let [[sh ph oh] (map *hashimpl* [s p o])]
      (if s
        (when-let [s (get-indexed conn "subj" sh)]
          (if p
            (when-let [p (get-indexed conn "pred" ph)]
              (when-let [objects (get-set conn (str "spo" sh ph))]
                (if o
                  (when (objects oh) [s p (get-indexed conn "obj" oh)])
                  (triples-sp conn s p objects))))
            (let [preds (get-set conn (str "sp" sh))]
              (if o
                (when-let [o (get-indexed conn "obj" oh)]
                  (mapcat
                   (fn [[p objects]] (if (some #(= oh %) objects) [[s (as-node p) o]]))
                   (zipmap
                    (get-many conn "pred" preds)
                    (map #(get-set conn (str "spo" sh %)) preds))))
                (reduce-triples conn triples-sp s sh "spo" "pred" preds)))))
        (if p
          (when-let [p (get-indexed conn "pred" ph)]
            (if o
              (when-let [subj (get-set conn (str "pos" ph oh))]
                (triples-po conn p (get-indexed conn "obj" oh) subj))
              (reduce-triples conn triples-po p ph "pos" "obj" (get-set conn (str "po" ph)))))
          (if o
            (when-let [preds (get-set conn (str "op" oh))]
              (reduce-triples conn triples-op (get-indexed conn "obj" oh) oh "ops" "pred" preds))
            (mapcat
             (fn [[sh s]]
               (reduce-triples
                conn triples-sp
                (get-indexed conn "subj" sh) sh
                "spo" "pred"
                (get-set conn (str "sp" sh))))
             (partition 2 (rexec conn (red/hgetall "subj"))))))))))

(defn make-store
  [conn] (RedisStore. conn))
