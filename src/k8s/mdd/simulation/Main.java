package k8s.mdd.simulation;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

public class Main {
	
	private static final Map<Integer, Example> examples = new HashMap<>();
	
	static {
		examples.put(1, new Example("Ecosystem with no co-evolution support", Main::example1));
		examples.put(2, new Example("Ecosystem with support for model and transformation co-evolution", Main::example2));
	}
	
	private static final void log(String message) {
		System.out.println(message);
	}
	
	private static final Artifact ecore = buildArtifact("ecore").build();
	private static final Artifact trafoMM = buildArtifact("trafoMM").build();
	private static final Artifact java = buildArtifact("java").build();
	private static final Artifact microservice = buildArtifact("microservice").withMetamodel(ecore.version()).build();
	private static final Artifact customerMicroservice = buildArtifact("customerMicroservice").withMetamodel(microservice.version()).build();
	private static final Artifact shoppingCartMicroservice = buildArtifact("shoppingCardMicroservice").withMetamodel(microservice.version()).build();
	private static final Artifact orderMicroservice = buildArtifact("orderMicroservice").withMetamodel(microservice.version()).build();
	private static final Artifact microserviceToSpringBoot = buildTransformation("microserviceToSpringBoot")
		.withMetamodel(trafoMM.version())
		.withDependency(microservice.version())
		.withTransformation(m -> {
			log("Generating Spring Boot microservices for model " + m.version());
			return Optional.of(buildArtifact(m.version().name() + "SpringBootGen").withMetamodel(java.version()).build());
		})
		.build();
	private static final Artifact microserviceToDotNet = buildTransformation("microserviceToDotNet")
		.withMetamodel(trafoMM.version())
		.withDependency(microservice.version())
		.withTransformation(m -> {
			log("Generating Dot Net microservices for model " + m.version());
			return Optional.of(buildArtifact(m.version().name() + "DotNetGen").withMetamodel(java.version()).build());
		})
		.build();
	private static final Artifact microserviceToPython = buildTransformation("microserviceToPython")
		.withMetamodel(trafoMM.version())
		.withDependency(microservice.version())
		.withTransformation(m -> {
			log("Generating Python microservices for model " + m.version());
			return Optional.of(buildArtifact(m.version().name() + "PythonGen").withMetamodel(java.version()).build());
		})
		.build();
	
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
		log("Executing example 1: Manual co-evolution");
		Repository repo = new Repository();
		repo.addArtifacts(ecore, java, microservice, microserviceToSpringBoot, microserviceToDotNet, customerMicroservice, shoppingCartMicroservice, orderMicroservice, microserviceToPython);
		repo.addArtifact(microservice);
		
	}
	
	public static void example2() {
		log("Executing example 2: Co-evolution with artifact-independent co-evolution model");
		Repository repo = new Repository();
		Artifact coEvM = buildArtifact("coEvM").build();
		Artifact coEvModelGen = buildTransformation("coEvModelGen").withDependency(ecore.version())
				.withTransformation(m -> {
					if (m.version().isInitialVersion()) {
						log("Don't create migration model for initial version of " + m.version());
						return Optional.empty();
					}
					log("Creating migration model for " + m.version());
					return Optional.of(buildCoEvolutionModel(m.version().name() + "-coEvM").withMetamodel(coEvM.version()).withChangedArtifact(m.version()).build());
				})
				.build();
		Artifact modelCoEvGen = buildTransformation("modelCoEvGen").withDependency(coEvM.version())
				.withTransformation(m -> {
					if (m instanceof CoEvolutionModel coev) {
						ArtifactVersion changedArtifact = coev.getChangedArtifact();
						log("Creating model migration for " + changedArtifact);
						return Optional.of(buildTransformation(changedArtifact.name() + "-model-migration").withDependency(changedArtifact.decrement())
							.withTransformation(instance -> {
								ArtifactVersion nextMetamodel = instance.getMetamodel().increment();
								if (!repo.containsByVersion(nextMetamodel)) {
									throw new IllegalStateException("Can't create migration to nonexistent meta model version");
								}
								Artifact migratedInstance = copyArtifact(instance).withMetamodel(nextMetamodel).build();
								log(String.format("Migrating model %s to %s", instance.version(), migratedInstance.version()));
								return Optional.of(migratedInstance);
							})
							.build());
					}
					return Optional.empty();
				}).build();
		Artifact transCoEvGen = buildTransformation("transCoEvGen").withDependency(coEvM.version())
				.withTransformation(m -> {
					if (m instanceof CoEvolutionModel coev) {
						ArtifactVersion changedArtifact = coev.getChangedArtifact();
						log("Creating transformation migration for " + changedArtifact);
						return Optional.of(buildTransformation(changedArtifact.name() + "-transformation-migration")
							.withDependency(trafoMM.version())
							.withTransformation(m1 -> {
								Artifact copy = copyArtifact(m1).build();
								Optional<Transformation> tOpt = copy.asTransformation();
								if (tOpt.isPresent()) {
									Transformation t = tOpt.get();
									ArtifactVersion previousVersion = changedArtifact.decrement();
									if (t.getDependencies().contains(previousVersion)) {
										log(String.format("Migrating transformation %s", t.version()));
										t.getDependencies().remove(previousVersion);
										t.getDependencies().add(changedArtifact);
										return Optional.of(t);
									}
								}
								return Optional.empty();
							})
							.build());
					}
					return Optional.empty();
				}).build();
		repo.addArtifacts(ecore, java, trafoMM);
		repo.addArtifact(coEvModelGen);
		repo.addArtifacts(modelCoEvGen, transCoEvGen, microservice, microserviceToSpringBoot, microserviceToDotNet, customerMicroservice, shoppingCartMicroservice, orderMicroservice, microserviceToPython);
		repo.addArtifact(microservice);
	}
	
	public static void onChange(Repository repo, Artifact a) {
		Optional.ofNullable(a.getMetamodel()).ifPresent(metamodel -> {
			for (Transformation transformation : repo.getDependingTransformations(metamodel)) {
				transformation.getTransformation().apply(a).ifPresent(repo::addArtifact);
			}
		});
		for (ArtifactVersion metamodel : a.asTransformation().map(Transformation::getDependencies).orElse(Collections.emptySet())) {
			for (Artifact model : repo.getInstances(metamodel)) {
				a.asTransformation().map(Transformation::getTransformation).flatMap(t -> t.apply(model)).ifPresent(repo::addArtifact);
			}
		}
	}
	
	public static interface Artifact {
		
		ArtifactVersion version();
		
		ArtifactVersion getMetamodel();
		
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
		
		Set<ArtifactVersion> getDependencies();
		
		Function<Artifact, Optional<Artifact>> getTransformation();
		
	}
	
	public static interface CoEvolutionModel extends Artifact {
		
		ArtifactVersion getChangedArtifact();
		
	}
	
	public static class ArtifactImpl implements Artifact {

		private final ArtifactVersion version;
		private final ArtifactVersion metamodel;
		
		public ArtifactImpl(ArtifactVersion version, ArtifactVersion metamodel) {
			this.version = version;
			this.metamodel = metamodel;
		}
		
		public ArtifactImpl(ArtifactVersion version) {
			this(version, null);
		}
		
		@Override
		public ArtifactVersion version() {
			return version;
		}
		
		@Override
		public ArtifactVersion getMetamodel() {
			return metamodel;
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
			return String.format("%s;%s\tmetamodel=%s", version, System.lineSeparator(), metamodel);
		}
		
	}
	
	public static class TransformationImpl extends ArtifactImpl implements Transformation {
		
		private final Set<ArtifactVersion> dependencies;
		private final Function<Artifact, Optional<Artifact>> transformation;
		
		public TransformationImpl(ArtifactVersion version, ArtifactVersion metamodel, Set<ArtifactVersion> dependencies, Function<Artifact, Optional<Artifact>> transformation) {
			super(version, metamodel);
			this.dependencies = dependencies;
			this.transformation = transformation;
		}

		public TransformationImpl(ArtifactVersion version, Set<ArtifactVersion> dependencies, Function<Artifact, Optional<Artifact>> transformation) {
			super(version);
			this.dependencies = dependencies;
			this.transformation = transformation;
		}

		@Override
		public Set<ArtifactVersion> getDependencies() {
			return dependencies;
		}

		@Override
		public Function<Artifact, Optional<Artifact>> getTransformation() {
			return transformation;
		}
		
		@Override
		public boolean equals(Object obj) {
			return super.equals(obj);
		}
		
		@Override
		public String toString() {
			return String.format("%s;%s\tdependencies=%s", super.toString(), System.lineSeparator(), dependencies);
		}
		
	}
	
	public static class CoEvolutionModelImpl extends ArtifactImpl implements CoEvolutionModel {
		
		private final ArtifactVersion changedArtifact;
		
		public CoEvolutionModelImpl(ArtifactVersion version, ArtifactVersion metamodel, ArtifactVersion changedArtifact) {
			super(version, metamodel);
			this.changedArtifact = changedArtifact;
		}
		
		public CoEvolutionModelImpl(ArtifactVersion version, ArtifactVersion changedArtifact) {
			super(version);
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
					.filter(m1 -> Objects.equals(m1.getMetamodel(), artifact))
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
		
		protected ArtifactVersion metamodel;
		
		protected abstract U getThis();
		
		public U withVersion(ArtifactVersion version) {
			this.version = version;
			return getThis();
		}
		
		public U withMetamodel(ArtifactVersion metamodel) {
			this.metamodel = metamodel;
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
			return new ArtifactImpl(version, metamodel);
		}
		
	}
	
	public static TransformationBuilder buildTransformation(String name) {
		return new TransformationBuilder(name);
	}
	
	public static TransformationBuilder buildTransformation(ArtifactVersion version) {
		return new TransformationBuilder(version);
	}
	
	public static class TransformationBuilder extends AbstractArtifactBuilder<Transformation, TransformationBuilder> {
		
		private final Set<ArtifactVersion> dependencies = new HashSet<>();
		private Function<Artifact, Optional<Artifact>> transformation;
		
		public TransformationBuilder(String name) {
			this.version = new ArtifactVersion(name, 0);
		}
		
		public TransformationBuilder(ArtifactVersion version) {
			this.version = version;
		}
		
		public TransformationBuilder withDependency(ArtifactVersion version) {
			this.dependencies.add(version);
			return this;
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
			return new TransformationImpl(version, metamodel, dependencies, transformation);
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
			return new CoEvolutionModelImpl(version, metamodel, changedArtifact);
		}
		
	}
	
	public static AbstractArtifactBuilder<?, ?> copyArtifact(Artifact artifact) {
		if (artifact instanceof CoEvolutionModel coevm) {
			return buildCoEvolutionModel(coevm.version()).withMetamodel(coevm.getMetamodel()).withChangedArtifact(coevm.getChangedArtifact());
		} else if (artifact instanceof Transformation t) {
			TransformationBuilder builder = buildTransformation(t.version()).withMetamodel(t.getMetamodel());
			t.getDependencies().forEach(builder::withDependency);
			return builder.withTransformation(t.getTransformation());
		} else {
			return buildArtifact(artifact.version()).withMetamodel(artifact.getMetamodel());
		}
	}
	
	public static record Example(String description, Runnable runnable) {}
	
}
