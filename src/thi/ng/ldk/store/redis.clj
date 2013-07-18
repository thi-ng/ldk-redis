(ns thi.ng.ldk.store.redis
  (:require
   [thi.ng.ldk.core.api :as api :refer [as-node]]
   [taoensso.carmine :as red]
   [clojure.pprint :refer [pprint]]))

(def redis-conn {:pool {} :spec {}})

(defmacro rexec [conn & body] `(red/wcar ~conn ~@body))

(defmacro get-indexed
  ([conn idx x] `(as-node (rexec ~conn (red/hget ~idx ~x))))
  ([conn s p o]
     `(map as-node
           (rexec ~conn
                  (red/hget "subj" ~s)
                  (red/hget "pred" ~p)
                  (red/hget "obj" ~o)))))

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

(defn triples*
  [conn f base-val base-hash inner-idx coll-idx coll]
  (mapcat
   (fn [c inner] (f conn base-val c inner))
   (get-many conn coll-idx coll)
   (map #(get-set conn (str inner-idx base-hash %)) coll)))

(defrecord RedisStore [conn]
  api/PModel
  (add-statement [this s p o]
    (let [[sh ph oh] (index-triple conn s p o)]
      (rexec conn
             (red/sadd (str "sp" sh) ph)
             (red/sadd (str "po" ph) oh)
             (red/sadd (str "op" oh) ph)
             (red/sadd (str "spo" sh ph) oh)
             (red/sadd (str "pos" ph oh) sh)
             (red/sadd (str "ops" oh ph) sh))
      this))
  (subject? [this x]
    (get-indexed conn "subj" (*hashimpl* x)))
  (predicate? [this x]
    (get-indexed conn "pred" (*hashimpl* x)))
  (object? [this x]
    (get-indexed conn "obj" (*hashimpl* x)))
  (add-prefix [this prefix uri]
    (rexec conn (red/hset "prefixes" prefix uri)) this)
  (add-prefix [this prefix-map]
    (rexec conn (apply red/hmset "prefixes" (flatten prefix-map)))
    this)
  (prefix-map [this]
    (apply hash-map (rexec conn (red/hgetall "prefixes"))))
  (select [this]
    (api/select this nil nil nil))
  (select
    [this s p o]
    (let [[sh ph oh] (map *hashimpl* [s p o])
          [si pi oi] (get-indexed conn sh ph oh)]
      (if s
        (when si
          (if p
            (when pi
              (if o
                (when (rexec conn (red/sismember (str "spo" sh ph) oh))
                  [si pi oi])
                (when-let [preds (seq (get-set conn (str "spo" sh ph)))]
                  (triples-sp conn si pi preds))))
            (let [preds (get-set conn (str "sp" sh))]
              (if o
                (when oi
                  (mapcat
                   (fn [p objects] (if (some #(= oh %) objects) [[si (as-node p) oi]]))
                   (get-many conn "pred" preds)
                   (map #(get-set conn (str "spo" sh %)) preds)))
                (triples* conn triples-sp s sh "spo" "pred" preds)))))
        (if p
          (when pi
            (if o
              (when-let [subj (seq (get-set conn (str "pos" ph oh)))]
                (triples-po conn pi oi subj))
              (triples* conn triples-po pi ph "pos" "obj" (get-set conn (str "po" ph)))))
          (if o
            (when-let [preds (seq (get-set conn (str "op" oh)))]
              (triples* conn triples-op oi oh "ops" "pred" preds))
            (mapcat
             (fn [[sh s]]
               (triples*
                conn triples-sp (as-node s) sh "spo" "pred" (get-set conn (str "sp" sh))))
             (partition 2 (rexec conn (red/hgetall "subj"))))))))))

(defn make-store
  [& {:as opts}] (RedisStore. (merge redis-conn {:spec (or opts {})})))
