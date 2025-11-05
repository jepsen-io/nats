(defproject jepsen.nats "0.1.0-SNAPSHOT"
  :description "Jepsen tests for the NATS Jetstream queuing system"
  :url "https://github.com/jepsen-io/nats"
  :license {:name "EPL-2.0 OR GPL-2.0-or-later WITH Classpath-exception-2.0"
            :url "https://www.eclipse.org/legal/epl-2.0/"}
  :dependencies [[org.clojure/clojure "1.12.0"]
                 [jepsen "0.3.10-SNAPSHOT"
                  :exclusions [com.fasterxml.jackson.core/jackson-databind
                               com.fasterxml.jackson.core/jackson-annotations
                               com.fasterxml.jackson.core/jackson-core]]
                 [io.jepsen/antithesis "0.1.0-SNAPSHOT"]
                 [io.nats/jnats "2.21.1"]
                 [cheshire "6.0.0"]]
  :repl-options {:init-ns jepsen.nats.repl}
  :jvm-opts ["-Xmx24g"
             "-Djava.awt.headless=true"
             "-server"]
  :profiles {}
  :main jepsen.nats.cli)

