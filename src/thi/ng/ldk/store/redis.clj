(ns thi.ng.ldk.store.redis
  (:require
   [thi.ng.ldk.core.api :as api :refer [as-node]]
   [taoensso.carmine :as red]
   [clojure.pprint :refer [pprint]]))

(def redis-conn {:pool {} :spec {}})

(defmacro rexec [conn & body] `(red/wcar ~conn ~@body))

(defmacro get-indexed
  [conn idx x] `(as-node (rexec ~conn (red/hget ~idx ~x))))

(defmacro get-coll
  ([conn idx x] `(rexec ~conn (red/hget ~idx ~x)))
  ([conn idx a b] `(rexec ~conn (red/hget ~idx (str ~a ~b)))))

(defmacro get-many [conn idx coll] `(rexec ~conn (apply red/hmget ~idx ~coll)))

(def ^:dynamic *hashimpl* (comp hash api/index-value))

(defn index-entity
  [conn idx e]
  (let [h (*hashimpl* e)] (rexec conn (red/hsetnx idx h e)) h))

(defn clear-store
  [conn]
  (rexec conn (red/flushdb)))

(defn add*
  [conn s p o]
  (let [sh (index-entity conn "subj" s)
        ph (index-entity conn "pred" p)
        oh (index-entity conn "obj" o)
        ksp (str sh ph)
        kpo (str ph oh)
        kop (str oh ph)
        sp (or (get-coll conn "sp" sh) #{})
        po (or (get-coll conn "po" ph) #{})
        op (or (get-coll conn "op" oh) #{})
        spo (or (get-coll conn "spo" ksp) #{})
        pos (or (get-coll conn "pos" kpo) #{})
        ops (or (get-coll conn "ops" kop) #{})]
    (rexec conn
           (red/hset "sp" sh (conj sp ph))
           (red/hset "po" ph (conj po oh))
           (red/hset "op" oh (conj op ph))
           (red/hset "spo" ksp (conj spo oh))
           (red/hset "pos" kpo (conj pos sh))
           (red/hset "ops" kop (conj ops sh)))
    [sh ph oh]))

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
   (zipmap (rexec conn (apply red/hmget coll-idx coll))
           (rexec conn (apply red/hmget inner-idx (map #(str base-hash %) coll))))))

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
              (when-let [objects (get-coll conn "spo" sh ph)]
                (if o
                  (when (objects oh) [s p (get-indexed conn "obj" oh)])
                  (triples-sp conn s p objects))))
            (let [preds (get-coll conn "sp" sh)]
              (if o
                (when-let [o (get-indexed conn "obj" oh)]
                  (mapcat
                   (fn [[p objects]] (if (some #(= oh %) objects) [[s (as-node p) o]]))
                   (zipmap
                    (get-many conn "pred" preds)
                    (get-many conn "spo" (map #(str sh %) preds)))))
                (reduce-triples conn triples-sp s sh "spo" "pred" preds)))))
        (if p
          (when-let [p (get-indexed conn "pred" ph)]
            (if o
              (when-let [subj (get-coll conn "pos" ph oh)]
                (triples-po conn p (get-indexed conn "obj" oh) subj))
              (reduce-triples conn triples-po p ph "pos" "obj" (get-coll conn "po" ph))))
          (if o
            (when-let [preds (get-coll conn "op" oh)]
              (reduce-triples conn triples-op (get-indexed conn "obj" oh) oh "ops" "pred" preds))
            (mapcat
             (fn [[sh s]]
               (reduce-triples
                conn triples-sp
                (get-indexed conn "subj" sh) sh
                "spo" "pred"
                (get-coll conn "sp" sh)))
             (partition 2 (rexec conn (red/hgetall "subj"))))))))))

(defn make-store
  [conn] (RedisStore. conn))
