package k8s.mdd.simulation;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

public class Main {
	
	private static record Example(String description, Runnable runnable) {}
	
	private static final Map<Integer, Example> examples = new HashMap<>();
	
	static {
		examples.put(1, new Example("Ecosystem with manual co-evolution support", Main::example1));
		examples.put(2, new Example("Ecosystem with support for semi-automatic model and transformation co-evolution", Main::example2));
		examples.put(3, new Example("Ecosystem with manual co-evolution support and plattform changes", Main::example3));
	}
	
	private static final void log(String message) {
		System.out.println(message);
	}
	
	private static final Repository repo = new Repository();
	
	// basic setup
	private static final Artifact ecore = buildArtifact("ecore").build();
	private static final Artifact trafoMM = buildArtifact("trafoMM").build();
	private static final Artifact java = buildArtifact("java").build();
	
	private static final Artifact springBootPlatform = buildArtifact("springBoot").build();
	private static final Artifact dotNetPlatform = buildArtifact("dotNet").build();
	private static final Artifact pythonPlatform = buildArtifact("python").build();
	
	private static final Artifact microservice = buildArtifact("microservice").withMetamodel(ecore.version()).build();
	private static final Artifact customerMicroservice = buildArtifact("customerMicroservice").withMetamodel(microservice.version()).build();
	private static final Artifact shoppingCartMicroservice = buildArtifact("shoppingCardMicroservice").withMetamodel(microservice.version()).build();
	private static final Artifact orderMicroservice = buildArtifact("orderMicroservice").withMetamodel(microservice.version()).build();
	
	private static final Artifact microserviceToSpringBoot = buildTransformation("microserviceToSpringBoot")
		.withMetamodel(trafoMM.version())
		.withDependency(microservice.version())
		.withDependency(springBootPlatform.version())
		.withTransformation(m -> {
			log("Generating Spring Boot microservices for model " + m.version());
			return Optional.of(buildArtifact(m.version().name() + "SpringBootGen").withMetamodel(java.version()).build());
		})
		.build();
	private static final Artifact microserviceToDotNet = buildTransformation("microserviceToDotNet")
		.withMetamodel(trafoMM.version())
		.withDependency(microservice.version())
		.withDependency(dotNetPlatform.version())
		.withTransformation(m -> {
			log("Generating Dot Net microservices for model " + m.version());
			return Optional.of(buildArtifact(m.version().name() + "DotNetGen").withMetamodel(java.version()).build());
		})
		.build();
	private static final Artifact microserviceToPython = buildTransformation("microserviceToPython")
		.withMetamodel(trafoMM.version())
		.withDependency(microservice.version())
		.withDependency(pythonPlatform.version())
		.withTransformation(m -> {
			log("Generating Python microservices for model " + m.version());
			return Optional.of(buildArtifact(m.version().name() + "PythonGen").withMetamodel(java.version()).build());
		})
		.build();
	
	// co-evolution support
	private static final Artifact coEvM = buildArtifact("coEvM").build();
	private static final Artifact coEvModelGen = buildTransformation("coEvModelGen").withDependency(ecore.version())
		.withTransformation(m -> {
			if (m.version().isInitialVersion()) {
				log("Don't create migration model for initial version of " + m.version());
				return Optional.empty();
			}
			log("Creating migration model for " + m.version());
			return Optional.of(buildCoEvolutionModel(m.version().name() + "-coEvM").withMetamodel(coEvM.version()).withChangedArtifact(m.version()).build());
		})
		.build();
	private static final Artifact modelCoEvGen = buildTransformation("modelCoEvGen").withDependency(coEvM.version())
		.withTransformation(m -> {
			if (m instanceof CoEvolutionModel coev) {
				ArtifactVersion changedArtifact = coev.getChangedArtifact();
				log("Creating model migration for " + changedArtifact);
				return Optional.of(buildTransformation(changedArtifact.name() + "-model-migration").withDependency(changedArtifact.decrement())
					.withTransformation(instance -> {
						// instances that are not conform to the previous version must not be migrated
						if (instance.getMetamodels().contains(changedArtifact.decrement())) {
							log(String.format("Migrating model %s", instance.version()));
							// the migration must update the meta model to the changed model
							Artifact migratedInstance = copyArtifact(instance).updateMetamodel(changedArtifact).build();
							return Optional.of(migratedInstance);
						}
						return Optional.empty();
						
					})
					.build());
			}
			return Optional.empty();
		}).build();
	private static final Artifact transCoEvGen = buildTransformation("transCoEvGen").withDependency(coEvM.version())
		.withTransformation(m -> {
			if (m instanceof CoEvolutionModel coev) {
				ArtifactVersion changedArtifact = coev.getChangedArtifact();
				log("Creating transformation migration for " + changedArtifact);
				return Optional.of(buildTransformation(changedArtifact.name() + "-transformation-migration")
					.withDependency(trafoMM.version())
					.withTransformation(m1 -> {
						// this condition is important to prevent a loop
						// only transformations that are dependent on the previous version must be migrated
						if (m1.getDependencies().contains(changedArtifact.decrement())) {
							log(String.format("Migrating transformation %s", m1.version()));
							// the migration must update the dependency to the changed model
							Artifact migratedTransformation = copyArtifact(m1).updateDependency(changedArtifact).build();
							return Optional.of(migratedTransformation);
						}
						return Optional.empty();
					})
					.build());
			}
			return Optional.empty();
		}).build();
	
	public static void main(String[] args) {
		if (args.length > 0) {
			if ("-h".equals(args[0]) || "--help".equals(args[0])) {
				printHelp();
			} else {
				int index = Integer.parseInt(args[0]);
				if (examples.containsKey(index)) {
					Example example = examples.get(index);
					log(String.format("Executing example %s: %s", index, example.description()));
					example.runnable().run();
				} else {
					printHelp();
				}
			}
		} else {
			printHelp();
		}
	}
	
	private static void printHelp() {
		log("Provide an integer as the first argument to run the workflow for an example ecosystem");
		examples.forEach((i, e) -> log(String.format("%s: %s", i, e.description())));
	}
	
	public static void example1() {
		repo.addArtifacts(ecore, java, microservice, microserviceToSpringBoot, microserviceToDotNet,
			customerMicroservice, shoppingCartMicroservice, orderMicroservice, microserviceToPython);
		repo.addArtifact(microservice);

	}

	public static void example2() {
		repo.addArtifacts(ecore, trafoMM, java, coEvModelGen, modelCoEvGen, transCoEvGen, microservice,
			microserviceToSpringBoot, microserviceToDotNet, customerMicroservice, shoppingCartMicroservice,
			orderMicroservice, microserviceToPython);
		repo.addArtifact(microservice);
	}

	public static void example3() {
		repo.addArtifacts(ecore, java, springBootPlatform, dotNetPlatform, pythonPlatform, microservice,
			microserviceToSpringBoot, microserviceToDotNet, customerMicroservice, shoppingCartMicroservice,
			orderMicroservice, microserviceToPython);
		// update the platform
		repo.addArtifact(springBootPlatform);
		// update the generator
		repo.addArtifact(copyArtifact(microserviceToSpringBoot).updateDependency(springBootPlatform.version().increment()).build());
	}
	
	public static void onChange(Repository repo, Artifact a) {
		for (ArtifactVersion metamodel : a.getMetamodels()) {
			for (Transformation transformation : repo.getDependingTransformations(metamodel)) {
				transformation.getTransformation().apply(a).ifPresent(repo::addArtifact);
			}
		}
		for (ArtifactVersion metamodel : a.getDependencies()) {
			for (Artifact model : repo.getInstances(metamodel)) {
				a.asTransformation().map(Transformation::getTransformation).flatMap(t -> t.apply(model)).ifPresent(repo::addArtifact);
			}
		}
	}
	
	public static interface Artifact {
		
		ArtifactVersion version();
		
		Set<ArtifactVersion> getMetamodels();
		
		Set<ArtifactVersion> getDependencies();
		
		Optional<Transformation> asTransformation();
		
	}
	
	public record ArtifactVersion(String name, int version) {
		
		public ArtifactVersion increment() {
			return new ArtifactVersion(name, version + 1);
		}
		
		public ArtifactVersion decrement() {
			if (isInitialVersion()) {
				throw new IllegalStateException("Can't create previous version for initial version");
			}
			return new ArtifactVersion(name, version - 1);
		}
		
		public boolean isInitialVersion() {
			return version == 0;
		}
		
	}
	
	public static interface Transformation extends Artifact {
		
		Function<Artifact, Optional<Artifact>> getTransformation();
		
	}
	
	public static interface CoEvolutionModel extends Artifact {
		
		ArtifactVersion getChangedArtifact();
		
	}
	
	public static class ArtifactImpl implements Artifact {

		private final ArtifactVersion version;
		private final Set<ArtifactVersion> metamodels;
		private final Set<ArtifactVersion> dependencies;
		
		public ArtifactImpl(ArtifactVersion version, Set<ArtifactVersion> metamodels, Set<ArtifactVersion> dependencies) {
			this.version = version;
			this.metamodels = metamodels;
			this.dependencies = dependencies;
		}
		
		@Override
		public ArtifactVersion version() {
			return version;
		}
		
		@Override
		public Set<ArtifactVersion> getMetamodels() {
			return metamodels;
		}

		@Override
		public Set<ArtifactVersion> getDependencies() {
			return dependencies;
		}
		
		@Override
		public Optional<Transformation> asTransformation() {
			return Optional.ofNullable(this)
				.filter(Transformation.class::isInstance)
				.map(Transformation.class::cast);
		}
		
		@Override
		public boolean equals(Object o) {
			if (o == this) {
				return true;
			}
			if (!(o instanceof Artifact)) {
				return false;
			}
			Artifact other = (Artifact) o;
			return Objects.equals(version, other.version());
		}
		
		@Override
		public String toString() {
			return String.format("%s;%s\tmetamodels=%s;%s\tdependencies=%s", version, System.lineSeparator(),
				metamodels, System.lineSeparator(), dependencies);
		}
		
	}
	
	public static class TransformationImpl extends ArtifactImpl implements Transformation {
		
		private final Function<Artifact, Optional<Artifact>> transformation;
		
		public TransformationImpl(ArtifactVersion version, Set<ArtifactVersion> metamodels, Set<ArtifactVersion> dependencies, Function<Artifact, Optional<Artifact>> transformation) {
			super(version, metamodels, dependencies);
			this.transformation = transformation;
		}

		@Override
		public Function<Artifact, Optional<Artifact>> getTransformation() {
			return transformation;
		}
		
		@Override
		public boolean equals(Object obj) {
			return super.equals(obj);
		}
		
	}
	
	public static class CoEvolutionModelImpl extends ArtifactImpl implements CoEvolutionModel {
		
		private final ArtifactVersion changedArtifact;
		
		public CoEvolutionModelImpl(ArtifactVersion version, Set<ArtifactVersion> metamodels, Set<ArtifactVersion> dependencies, ArtifactVersion changedArtifact) {
			super(version, metamodels, dependencies);
			this.changedArtifact = changedArtifact;
		}
		
		@Override
		public ArtifactVersion getChangedArtifact() {
			return changedArtifact;
		}
		
		@Override
		public boolean equals(Object obj) {
			return super.equals(obj);
		}
		
		@Override
		public String toString() {
			return String.format("%s;%s\tchangedArtifact=%s", super.toString(), System.lineSeparator(), changedArtifact);
		}
		
	}
	
	public static class Repository {
		
		private Map<ArtifactVersion, Artifact> artifactsByVersion = new HashMap<>();
		
		public Set<Artifact> getInstances(ArtifactVersion artifact) {
			return artifactsByVersion.values().stream()
				// find any model that has declared the argument as meta model
				.filter(m1 -> m1.getMetamodels().contains(artifact))
				.collect(Collectors.toSet());
		}
		
		public Set<Transformation> getDependingTransformations(ArtifactVersion artifact) {
			return artifactsByVersion.values().stream().map(Artifact::asTransformation)
				// find any transformation that has declared the argument as a dependency
				.filter(Optional::isPresent)
				.map(Optional::get)
				.filter(t -> t.getDependencies().contains(artifact))
				.collect(Collectors.toSet());
		}
		
		public Optional<Artifact> getByVersion(ArtifactVersion version) {
			return Optional.ofNullable(artifactsByVersion.get(version));
		}
		
		public boolean containsByVersion(ArtifactVersion version) {
			return artifactsByVersion.containsKey(version);
		}

		public Optional<Artifact> getNextVersion(Artifact a) {
			return Optional.ofNullable(artifactsByVersion.get(a.version().increment()));
		}
		
		public void addArtifact(Artifact a) {
			Artifact newVersion = artifactsByVersion.containsKey(a.version()) ? copyArtifact(a).withVersion(a.version().increment()).build() : a;
			artifactsByVersion.put(newVersion.version(), newVersion);
			log("Added " + newVersion);
			onChange(this, newVersion);
		}
		
		public void addArtifacts(Artifact... a) {
			Arrays.asList(a).forEach(this::addArtifact);
		}
		
	}
	
	public abstract static class AbstractArtifactBuilder<T extends Artifact, U extends AbstractArtifactBuilder<T, U>> {
		
		protected ArtifactVersion version;
		
		protected final Set<ArtifactVersion> metamodels = new HashSet<>();
		
		protected final Set<ArtifactVersion> dependencies = new HashSet<>();
		
		protected abstract U getThis();
		
		public U withVersion(ArtifactVersion version) {
			this.version = version;
			return getThis();
		}
		
		public U withMetamodel(ArtifactVersion metamodel) {
			this.metamodels.add(metamodel);
			return getThis();
		}
		
		public U updateMetamodel(ArtifactVersion version) {
			if (this.metamodels.remove(version.decrement())) {
				this.metamodels.add(version);
			}
			return getThis();
		}
		
		public U withDependency(ArtifactVersion version) {
			this.dependencies.add(version);
			return getThis();
		}
		
		public U updateDependency(ArtifactVersion version) {
			if (this.dependencies.remove(version.decrement())) {
				this.dependencies.add(version);
			}
			return getThis();
		}
		
		public abstract T build();
		
	}
	
	public static ArtifactBuilder buildArtifact(String name) {
		return new ArtifactBuilder(name);
	}
	
	public static ArtifactBuilder buildArtifact(ArtifactVersion version) {
		return new ArtifactBuilder(version);
	}
	
	public static class ArtifactBuilder extends AbstractArtifactBuilder<Artifact, ArtifactBuilder> {
		
		public ArtifactBuilder(String name) {
			this.version = new ArtifactVersion(name, 0);
		}
		
		public ArtifactBuilder(ArtifactVersion version) {
			this.version = version;
		}
		
		@Override
		protected ArtifactBuilder getThis() {
			return this;
		}

		@Override
		public Artifact build() {
			return new ArtifactImpl(version, metamodels, dependencies);
		}
		
	}
	
	public static TransformationBuilder buildTransformation(String name) {
		return new TransformationBuilder(name);
	}
	
	public static TransformationBuilder buildTransformation(ArtifactVersion version) {
		return new TransformationBuilder(version);
	}
	
	public static class TransformationBuilder extends AbstractArtifactBuilder<Transformation, TransformationBuilder> {
		
		private Function<Artifact, Optional<Artifact>> transformation;
		
		public TransformationBuilder(String name) {
			this.version = new ArtifactVersion(name, 0);
		}
		
		public TransformationBuilder(ArtifactVersion version) {
			this.version = version;
		}
		
		public TransformationBuilder withTransformation(Function<Artifact, Optional<Artifact>> transformation) {
			this.transformation = transformation;
			return this;
		}
		
		@Override
		protected TransformationBuilder getThis() {
			return this;
		}

		@Override
		public Transformation build() {
			return new TransformationImpl(version, metamodels, dependencies, transformation);
		}
		
	}
	
	public static CoEvolutionModelBuiler buildCoEvolutionModel(String name) {
		return new CoEvolutionModelBuiler(name);
	}
	
	public static CoEvolutionModelBuiler buildCoEvolutionModel(ArtifactVersion version) {
		return new CoEvolutionModelBuiler(version);
	}
	
	public static class CoEvolutionModelBuiler extends AbstractArtifactBuilder<CoEvolutionModel, CoEvolutionModelBuiler> {
		
		private ArtifactVersion changedArtifact;
		
		public CoEvolutionModelBuiler(String name) {
			this.version = new ArtifactVersion(name, 0);
		}
		
		public CoEvolutionModelBuiler(ArtifactVersion version) {
			this.version = version;
		}
		
		public CoEvolutionModelBuiler withChangedArtifact(ArtifactVersion changedArtifact) {
			this.changedArtifact = changedArtifact;
			return this;
		}
		
		@Override
		protected CoEvolutionModelBuiler getThis() {
			return this;
		}

		@Override
		public CoEvolutionModel build() {
			return new CoEvolutionModelImpl(version, metamodels, dependencies, changedArtifact);
		}
		
	}
	
	public static AbstractArtifactBuilder<?, ?> copyArtifact(Artifact artifact) {
		AbstractArtifactBuilder<?, ?> builder;
		if (artifact instanceof CoEvolutionModel coevm) {
			builder = buildCoEvolutionModel(coevm.version()).withChangedArtifact(coevm.getChangedArtifact());
		} else if (artifact instanceof Transformation t) {
			builder = buildTransformation(t.version()).withTransformation(t.getTransformation());
		} else {
			builder = buildArtifact(artifact.version());
		}
		artifact.getMetamodels().forEach(builder::withMetamodel);
		artifact.getDependencies().forEach(builder::withDependency);
		return builder;
	}
	
}
