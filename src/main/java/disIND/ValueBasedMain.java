package disIND;

import akka.actor.typed.ActorSystem;
import akka.cluster.Member;
import akka.cluster.MemberStatus;
import akka.cluster.typed.Cluster;
import com.typesafe.config.ConfigFactory;
import disIND.valueBased.actors.INDGuardian;
import disIND.valueBased.actors.StartGuardian;
import disIND.valueBased.actors.StartGuardian.RuntimeHandle;
import disIND.valueBased.actors.StartGuardian.Settings;
import disIND.valueBased.dataset.DataLoader;
import disIND.valueBased.model.SharedModel.BDCommand;
import disIND.valueBased.protocol.StartProtocol;
import disIND.valueBased.utility.UserConfig;

import java.util.Locale;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public final class ValueBasedMain {

    private ValueBasedMain() {
    }

    public static void main(String[] args) throws Exception {
        UserConfig.initialize(args);
        String nodeRole = nodeRole();
        int expectedMembers = positiveEnvironmentInt("DIS_IND_EXPECTED_CLUSTER_SIZE", 1);
        int expectedWorkers = nonNegativeEnvironmentInt(
                "DIS_IND_EXPECTED_WORKERS", Math.max(0, expectedMembers - 1));
        int startTimeoutSeconds = positiveEnvironmentInt("DIS_IND_CLUSTER_START_TIMEOUT_SECONDS", 120);

        INDGuardian.Config coordinatorConfig = null;
        if ("coordinator".equals(nodeRole)) {
            coordinatorConfig = DataLoader.discoverConfig(UserConfig.INPUT_DIR, UserConfig.DATA_ORIENTATION,
                    UserConfig.CANDIDATE_TRACKING);
            System.out.printf("[Main] Discovered %,d tables and %,d columns%n",
                    coordinatorConfig.metadata().tableNames().size(), coordinatorConfig.metadata().totalCols());
        }

        CompletableFuture<RuntimeHandle> runtimeReady = new CompletableFuture<>();
        Settings settings = new Settings(nodeRole, coordinatorConfig, expectedWorkers);
        ActorSystem<StartProtocol.Command> system = ActorSystem.create(
                StartGuardian.create(settings, runtimeReady), "disIND", ConfigFactory.load());
        System.out.println("[Main] Value-based bootstrap started as " + nodeRole + ".");

        try {
            awaitClusterReady(system, expectedMembers, startTimeoutSeconds);
            RuntimeHandle runtime = awaitRuntime(runtimeReady, startTimeoutSeconds);

            if ("worker".equals(nodeRole)) {
                System.out.println("[Main] Worker initialized from coordinator metadata; waiting for completion.");
                awaitCoordinatorDeparture(system);
                runtime.guardian().tell(new BDCommand.Shutdown());
                system.terminate();
                system.getWhenTerminated().toCompletableFuture().get(30, TimeUnit.SECONDS);
                return;
            }

            DataLoader.run(runtime.guardian(), system, runtime.config().metadata(), UserConfig.INPUT_DIR,
                    UserConfig.CHUNK_SIZE, 5, UserConfig.OUTPUT_DIR, runtime.config().orientation());
        } finally {
            if (!system.getWhenTerminated().toCompletableFuture().isDone()) {
                System.out.println("[Main] Terminating actor system.");
                system.terminate();
                system.getWhenTerminated().toCompletableFuture().get(30, TimeUnit.SECONDS);
            }
            System.out.println("[Main] System terminated.");
        }
    }

    private static String nodeRole() {
        String role = System.getenv().getOrDefault("DIS_IND_NODE_ROLE", "coordinator")
                .toLowerCase(Locale.ROOT);
        if (!role.equals("coordinator") && !role.equals("worker"))
            throw new IllegalArgumentException("DIS_IND_NODE_ROLE must be coordinator or worker: " + role);
        return role;
    }

    private static RuntimeHandle awaitRuntime(CompletableFuture<RuntimeHandle> ready, int timeoutSeconds)
            throws Exception {
        try {
            return ready.get(timeoutSeconds, TimeUnit.SECONDS);
        } catch (TimeoutException exception) {
            throw new IllegalStateException("Timed out waiting for workers to install dataset metadata", exception);
        }
    }

    private static void awaitCoordinatorDeparture(ActorSystem<?> system) throws InterruptedException {
        String coordinatorHost = System.getenv().getOrDefault("AKKA_SEED_HOST", "coordinator");
        Cluster cluster = Cluster.get(system);
        String addressFragment = "@" + coordinatorHost + ":";
        while (true) {
            boolean up = false;
            boolean unreachable = false;
            for (Member member : cluster.state().getMembers()) {
                if (member.status().equals(MemberStatus.up())
                        && member.address().toString().contains(addressFragment)) {
                    up = true;
                    break;
                }
            }
            for (Member member : cluster.state().getUnreachable()) {
                if (member.address().toString().contains(addressFragment)) {
                    unreachable = true;
                    break;
                }
            }
            if (!up || unreachable) {
                System.out.println("[Main] Coordinator left the cluster; stopping worker.");
                return;
            }
            Thread.sleep(1000);
        }
    }

    private static void awaitClusterReady(ActorSystem<?> system, int expectedMembers, int timeoutSeconds)
            throws Exception {
        Cluster cluster = Cluster.get(system);
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(timeoutSeconds);
        while (System.nanoTime() < deadline) {
            int upMembers = 0;
            for (Member member : cluster.state().getMembers()) {
                if (member.status().equals(MemberStatus.up()))
                    upMembers++;
            }
            if (upMembers >= expectedMembers) {
                System.out.printf("[Main] Cluster ready with %d member(s).%n", upMembers);
                return;
            }
            Thread.sleep(500);
        }
        throw new IllegalStateException("Timed out waiting for " + expectedMembers + " cluster member(s)");
    }

    private static int positiveEnvironmentInt(String name, int fallback) {
        int value = Integer.parseInt(System.getenv().getOrDefault(name, Integer.toString(fallback)));
        if (value <= 0)
            throw new IllegalArgumentException(name + " must be greater than zero: " + value);
        return value;
    }

    private static int nonNegativeEnvironmentInt(String name, int fallback) {
        int value = Integer.parseInt(System.getenv().getOrDefault(name, Integer.toString(fallback)));
        if (value < 0)
            throw new IllegalArgumentException(name + " cannot be negative: " + value);
        return value;
    }
}
