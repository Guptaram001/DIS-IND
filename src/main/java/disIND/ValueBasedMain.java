package disIND;

import akka.actor.typed.ActorSystem;
import akka.cluster.MemberStatus;
import akka.cluster.Member;
import akka.cluster.typed.Cluster;
import com.typesafe.config.ConfigFactory;
import disIND.valueBased.actors.INDGuardian;
import disIND.valueBased.dataset.DataLoader;
import disIND.valueBased.model.SharedModel;
import disIND.valueBased.utility.UserConfig;

import java.util.concurrent.TimeUnit;
import java.util.Locale;

public class ValueBasedMain {

    private ValueBasedMain() {
    }

    public static void main(String[] args) throws Exception {

        UserConfig.initialize(args);
        String nodeRole = System.getenv().getOrDefault("DIS_IND_NODE_ROLE", "coordinator")
                .toLowerCase(Locale.ROOT);
        String inputDir = UserConfig.inputDir;
        int batchSize = UserConfig.BATCH_SIZE;
        int timeoutSec = 5;
        String outputFile = UserConfig.outputDir;

        INDGuardian.Config cfg = DataLoader.discoverConfig(inputDir, UserConfig.DATA_ORIENTATION,
                UserConfig.CANDIDATE_TRACKING);
        System.out.println("[Main] Discovered config: " + cfg);
        ActorSystem<SharedModel.BDCommand> system = ActorSystem.create(INDGuardian.create(cfg), "disIND",
                ConfigFactory.load());
        System.out.println("[Main] Value Based INDGuardian started as " + nodeRole + ".");

        awaitClusterReady(system);

        if ("worker".equals(nodeRole)) {
            System.out.println("[Main] Worker is ready; ingestion is owned by the coordinator.");
            awaitCoordinatorDeparture(system);
            return;
        }
        if (!"coordinator".equals(nodeRole)) {
            system.terminate();
            throw new IllegalArgumentException(
                    "DIS_IND_NODE_ROLE must be either 'coordinator' or 'worker': " + nodeRole);
        }

        try {
            DataLoader.run(system, cfg.metadata(), inputDir, batchSize, timeoutSec, outputFile, cfg.orientation());
        } finally {
            System.out.println("[Main] Terminating actor system.");
            system.terminate();
            system.getWhenTerminated()
                    .toCompletableFuture()
                    .get(10, TimeUnit.SECONDS);
            System.out.println("[Main] System terminated.");
        }
    }

    private static void awaitCoordinatorDeparture(ActorSystem<SharedModel.BDCommand> system) throws Exception {
        String coordinatorHost = System.getenv().getOrDefault("AKKA_SEED_HOST", "coordinator");
        Cluster cluster = Cluster.get(system);

        while (true) {
            boolean coordinatorIsUp = false;
            boolean coordinatorIsUnreachable = false;
            String coordinatorAddressFragment = "@" + coordinatorHost + ":";
            for (Member member : cluster.state().getMembers()) {
                if (member.status().equals(MemberStatus.up())
                        && member.address().toString().contains(coordinatorAddressFragment)) {
                    coordinatorIsUp = true;
                    break;
                }
            }
            for (Member member : cluster.state().getUnreachable()) {
                if (member.address().toString().contains(coordinatorAddressFragment)) {
                    coordinatorIsUnreachable = true;
                    break;
                }
            }
            if (!coordinatorIsUp || coordinatorIsUnreachable) {
                System.out.println("[Main] Coordinator left the cluster; stopping worker.");
                system.terminate();
                system.getWhenTerminated().toCompletableFuture().get();
                return;
            }
            Thread.sleep(1000);
        }
    }

    private static void awaitClusterReady(ActorSystem<SharedModel.BDCommand> system) throws Exception {
        int expectedMembers = Integer.parseInt(
                System.getenv().getOrDefault("DIS_IND_EXPECTED_CLUSTER_SIZE", "1"));
        int timeoutSeconds = Integer.parseInt(
                System.getenv().getOrDefault("DIS_IND_CLUSTER_START_TIMEOUT_SECONDS", "120"));
        Cluster cluster = Cluster.get(system);
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(timeoutSeconds);

        while (System.nanoTime() < deadline) {
            int upMembers = 0;
            for (Member member : cluster.state().getMembers()) {
                if (member.status().equals(MemberStatus.up())) {
                    upMembers++;
                }
            }
            if (upMembers >= expectedMembers) {
                System.out.printf("[Main] Cluster ready with %d member(s).%n", upMembers);
                return;
            }
            Thread.sleep(500);
        }
        System.out.println("[Main] Coordinator left the cluster; " + "writing worker metrics and stopping.");

        system.tell(new SharedModel.BDCommand.Shutdown());
        system.getWhenTerminated().toCompletableFuture().get();

        return;
    }

}
