(defproject jepsen.nats "0.1.0-SNAPSHOT"
  :description "Jepsen tests for the NATS Jetstream queuing system"
  :url "https://github.com/jepsen-io/nats"
  :license {:name "EPL-2.0 OR GPL-2.0-or-later WITH Classpath-exception-2.0"
            :url "https://www.eclipse.org/legal/epl-2.0/"}
  :dependencies [[org.clojure/clojure "1.12.0"]
                 [jepsen "0.3.9"]
                 [io.nats/jnats "2.21.1"]]
  :repl-options {:init-ns jepsen.nats.repl}
  :jvm-opts ["-Xmx24g"
             "-Djava.awt.headless=true"
             "-server"]
  :profiles {}
  :main jepsen.nats.cli)

