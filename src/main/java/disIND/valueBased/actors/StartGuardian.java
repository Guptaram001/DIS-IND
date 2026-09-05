package disIND.valueBased.actors;

import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import akka.actor.typed.receptionist.Receptionist;
import akka.actor.typed.receptionist.ServiceKey;
import akka.actor.typed.javadsl.AbstractBehavior;
import akka.actor.typed.javadsl.ActorContext;
import akka.actor.typed.javadsl.Behaviors;
import akka.actor.typed.javadsl.Receive;
import akka.cluster.typed.Cluster;
import disIND.valueBased.model.SharedModel.BDCommand;
import disIND.valueBased.protocol.StartProtocol;
import disIND.valueBased.protocol.StartProtocol.ConfigServiceListing;
import disIND.valueBased.protocol.StartProtocol.DatasetConfig;
import disIND.valueBased.protocol.StartProtocol.InstallConfig;
import disIND.valueBased.protocol.StartProtocol.RequestConfig;
import disIND.valueBased.protocol.StartProtocol.WorkerReady;

import java.util.HashSet;
import java.util.Locale;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

public final class StartGuardian extends AbstractBehavior<StartProtocol.Command> {

    public static final ServiceKey<StartProtocol.Command> CONFIG_SERVICE = ServiceKey
            .create(StartProtocol.Command.class, "value-based-config-service");

    public record RuntimeHandle(ActorRef<BDCommand> guardian, INDGuardian.Config config) {
    }

    public record Settings(String nodeRole, INDGuardian.Config coordinatorConfig, int expectedWorkers) {
        public Settings {
            nodeRole = Objects.requireNonNull(nodeRole, "nodeRole").toLowerCase(Locale.ROOT);
            if (!nodeRole.equals("coordinator") && !nodeRole.equals("worker"))
                throw new IllegalArgumentException("nodeRole must be coordinator or worker: " + nodeRole);
            if (nodeRole.equals("coordinator") && coordinatorConfig == null)
                throw new IllegalArgumentException("coordinatorConfig is required on the coordinator");
            if (expectedWorkers < 0)
                throw new IllegalArgumentException("expectedWorkers cannot be negative");
        }
    }

    public static Behavior<StartProtocol.Command> create(
            Settings settings, CompletableFuture<RuntimeHandle> ready) {
        return Behaviors.setup(context -> new StartGuardian(context, settings, ready));
    }

    private final Settings settings;
    private final CompletableFuture<RuntimeHandle> ready;
    private final String workerId;
    private final Set<String> readyWorkers = new HashSet<>();
    private ActorRef<BDCommand> indGuardian;
    private ActorRef<StartProtocol.Command> coordinator;

    private StartGuardian(ActorContext<StartProtocol.Command> context,
            Settings settings, CompletableFuture<RuntimeHandle> ready) {
        super(context);

        this.settings = Objects.requireNonNull(settings, "settings");
        this.ready = Objects.requireNonNull(ready, "ready");
        this.workerId = Cluster.get(context.getSystem()).selfMember().address().toString();

        if (settings.nodeRole().equals("coordinator")) {
            indGuardian = context.spawn(INDGuardian.create(settings.coordinatorConfig()), "ind-guardian");
            context.getSystem().receptionist().tell(Receptionist.register(CONFIG_SERVICE, context.getSelf()));
            completeCoordinatorWhenReady();
        } else {
            ActorRef<Receptionist.Listing> adapter = context.messageAdapter(Receptionist.Listing.class,
                    listing -> new ConfigServiceListing(listing));
            context.getSystem().receptionist().tell(Receptionist.subscribe(CONFIG_SERVICE, adapter));
        }
    }

    @Override
    public Receive<StartProtocol.Command> createReceive() {
        return newReceiveBuilder()
                .onMessage(ConfigServiceListing.class, this::onListing)
                .onMessage(RequestConfig.class, this::onRequestConfig)
                .onMessage(InstallConfig.class, this::onInstallConfig)
                .onMessage(WorkerReady.class, this::onWorkerReady)
                .build();
    }

    private Behavior<StartProtocol.Command> onListing(ConfigServiceListing message) {
        if (!settings.nodeRole().equals("worker") || indGuardian != null)
            return this;

        message.listing().getServiceInstances(CONFIG_SERVICE).stream().findFirst().ifPresent(service -> {
            coordinator = service;
            service.tell(new RequestConfig(workerId, getContext().getSelf()));
        });
        return this;
    }

    private Behavior<StartProtocol.Command> onRequestConfig(RequestConfig message) {
        if (!settings.nodeRole().equals("coordinator"))
            return this;
        INDGuardian.Config config = settings.coordinatorConfig();

        message.replyTo().tell(new InstallConfig(new DatasetConfig(
                config.metadata(), config.orientation(), config.candidateTracking())));
        return this;
    }

    private Behavior<StartProtocol.Command> onInstallConfig(InstallConfig message) {

        if (!settings.nodeRole().equals("worker") || indGuardian != null)
            return this;
        DatasetConfig received = message.config();

        INDGuardian.Config config = INDGuardian.Config.withAll(
                received.metadata(), received.orientation(), received.candidateTracking());
        indGuardian = getContext().spawn(INDGuardian.create(config), "ind-guardian");

        if (coordinator == null) {
            ready.completeExceptionally(new IllegalStateException("Coordinator unavailable while installing config"));
            return this;
        }
        coordinator.tell(new WorkerReady(workerId));
        ready.complete(new RuntimeHandle(indGuardian, config));

        getContext().getLog().info("Worker runtime initialized from coordinator metadata: worker={}", workerId);

        return this;
    }

    private Behavior<StartProtocol.Command> onWorkerReady(WorkerReady message) {
        if (!settings.nodeRole().equals("coordinator"))
            return this;
        if (readyWorkers.add(message.workerId()))
            getContext().getLog().info("Worker ready: {} ({}/{})", message.workerId(), readyWorkers.size(),
                    settings.expectedWorkers());
        completeCoordinatorWhenReady();
        return this;
    }

    private void completeCoordinatorWhenReady() {
        if (settings.nodeRole().equals("coordinator") && readyWorkers.size() >= settings.expectedWorkers()) {
            ready.complete(new RuntimeHandle(indGuardian, settings.coordinatorConfig()));
        }
    }
}
