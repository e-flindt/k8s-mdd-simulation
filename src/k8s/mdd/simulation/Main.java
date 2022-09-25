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
	
	private static boolean debug = false;
	
	private static record Example(String description, Runnable runnable) {}
	
	private static final Map<Integer, Example> examples = new HashMap<>();
	
	static {
		examples.put(1, new Example("Ecosystem with manual co-evolution support where a meta model is changed", Main::example1));
		examples.put(2, new Example("Ecosystem with support for semi-automatic model and transformation co-evolution where a meta model is changed", Main::example2));
		examples.put(3, new Example("Ecosystem with support for semi-automatic model and transformation co-evolution where a platform is changed", Main::example3));
		examples.put(4, new Example("Ecosystem with support for semi-automatic model and transformation co-evolution where the java version is changed", Main::example4));
	}
	
	private static final void log(String message) {
		System.out.println(message);
	}
	
	private static final Repository repo = new RepositoryImpl();
	
	// basic setup
	private static final Artifact executable = buildArtifact("executable").build();
	private static final Artifact deploymentPipeline = buildTransformation("deploymentPipeline")
		.withInput(executable.version())
		.withTransformation(m -> {
			log("[DEPLOY] Integration testing and deploying " + m.version());
			return Optional.empty();
		})
		.build();
	private static final Artifact sourceCode = buildArtifact("sourceCode").build();
	private static final Artifact ecore = buildArtifact("ecore").build();
	private static final Artifact trafoMM = buildArtifact("trafoMM").build();
	
	// java stuff
	private static final Artifact java = buildArtifact("java")
		.withMetamodel(sourceCode.version()).build();	
	private static final Artifact javaBuildPipeline = buildTransformation("javaBuildPipeline")
		.withInput(java.version())
		.withOutput(executable.version())
		.withTransformation(m -> {
			log("[BUILD] Unit testing and building " + m.version());
			return Optional.of(buildArtifact(String.format("%sVer%s.jar", m.version().name(), m.version().version()))
				.withMetamodel(executable.version())
				.build());
		})
		.build();
	
	// platforms
	private static final Artifact springBootPlatform = buildArtifact("springBoot").build();
	private static final Artifact dotNetPlatform = buildArtifact("dotNet").build();
	private static final Artifact pythonPlatform = buildArtifact("python").build();
	
	// microservice meta model and instances
	private static final Artifact microservice = buildArtifact("microservice")
		.withMetamodel(ecore.version()).build();
	private static final Artifact customerMicroservice = buildArtifact("customerMicroservice")
		.withMetamodel(microservice.version()).build();
	private static final Artifact shoppingCartMicroservice = buildArtifact("shoppingCardMicroservice")
		.withMetamodel(microservice.version()).build();
	private static final Artifact orderMicroservice = buildArtifact("orderMicroservice")
		.withMetamodel(microservice.version()).build();
	
	// microservice generators
	private static final Artifact microserviceToSpringBoot = buildTransformation("microserviceToSpringBoot")
		.withMetamodel(trafoMM.version())
		.withInput(microservice.version())
		.withOutput(springBootPlatform.version())
		.withTransformation(m -> {
			log("[M2T] Generating Spring Boot microservices for model " + m.version());
			return Optional.of(buildArtifact(m.version().name() + "SpringBootGen")
				.withMetamodel(java.version()).build());
		})
		.build();
	private static final Artifact microserviceToDotNet = buildTransformation("microserviceToDotNet")
		.withMetamodel(trafoMM.version())
		.withInput(microservice.version())
		.withOutput(dotNetPlatform.version())
		.withTransformation(m -> {
			log("[M2T] Generating Dot Net microservices for model " + m.version());
			return Optional.of(buildArtifact(m.version().name() + "DotNetGen")
				.withMetamodel(sourceCode.version()).build());
		})
		.build();
	private static final Artifact microserviceToPython = buildTransformation("microserviceToPython")
		.withMetamodel(trafoMM.version())
		.withInput(microservice.version())
		.withOutput(pythonPlatform.version())
		.withTransformation(m -> {
			log("[M2T] Generating Python microservices for model " + m.version());
			return Optional.of(buildArtifact(m.version().name() + "PythonGen")
				.withMetamodel(sourceCode.version()).build());
		})
		.build();
	
	// co-evolution support
	private static final Artifact coEvM = buildArtifact("coEvM").build();
	private static final Artifact coEvModelGen = buildTransformation("coEvModelGen")
		.withInput(ecore.version())
		.withOutput(coEvM.version())
		.withTransformation(m -> {
			if (m.version().isInitialVersion()) {
				log("[CoEv] Don't create migration model for initial version of " + m.version());
				return Optional.empty();
			}
			log("[CoEv] Creating migration model for " + m.version());
			return Optional.of(buildCoEvolutionModel(m.version().name() + "-coEvM")
				.withMetamodel(coEvM.version())
				.withChangedArtifact(m.version()).build());
		})
		.build();
	private static final Artifact modelCoEvGen = buildTransformation("modelCoEvGen")
		.withInput(coEvM.version())
		.withOutput(trafoMM.version())
		.withTransformation(m -> {
			if (m instanceof CoEvolutionModel coev) {
				ArtifactVersion changedArtifact = coev.getChangedArtifact();
				log("[CoEv] Creating model migration for " + changedArtifact);
				return Optional.of(buildTransformation(changedArtifact.name() + "-model-migration")
					// previous meta model version is the input
					.withInput(changedArtifact.decrement())
					// new meta model version is the output
					.withOutput(changedArtifact)
					.withTransformation(instance -> {
						// instances that are not conform to the previous version must not be migrated
						if (instance.getMetamodels().contains(changedArtifact.decrement())) {
							log(String.format("[M2M] Migrating model %s", instance.version()));
							// the migration must update the meta model to the changed model
							Artifact migratedInstance = copyArtifact(instance)
								.updateMetamodel(changedArtifact).build();
							return Optional.of(migratedInstance);
						}
						return Optional.empty();
						
					})
					.build());
			}
			return Optional.empty();
		}).build();
	private static final Artifact trafoCoEvGen = buildTransformation("trafoCoEvGen")
		.withInput(coEvM.version())
		.withOutput(trafoMM.version())
		.withTransformation(m -> {
			if (m instanceof CoEvolutionModel coev) {
				ArtifactVersion changedArtifact = coev.getChangedArtifact();
				log("[CoEv] Creating transformation migration for " + changedArtifact);
				return Optional.of(buildTransformation(changedArtifact.name() + "-transformation-migration")
					// signals that this transformation transforms other transformation
					// this is a higher order transformation
					.withInput(trafoMM.version())
					.withOutput(trafoMM.version())
					.withTransformation(m1 -> {
						// this condition is important to prevent a loop
						// only transformations that are dependent on the previous version must be migrated
						if (m1.getInputs().contains(changedArtifact.decrement())
							|| m1.getOutputs().contains(changedArtifact.decrement())) {
							log(String.format("[M2M] Migrating transformation %s", m1.version()));
							// the migration must update the dependency to the changed model
							Artifact migratedTransformation = copyArtifact(m1)
								.updateDependency(changedArtifact).build();
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
			if (args.length > 1 && "-d".equals(args[1])) {
				debug = true;
			}
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
		log("Use the -d flag as the second argument to get verbose output");
		examples.forEach((i, e) -> log(String.format("%s: %s", i, e.description())));
	}
	
	public static void example1() {
		repo.commit(executable, deploymentPipeline, sourceCode, ecore, trafoMM, java, javaBuildPipeline, springBootPlatform, dotNetPlatform, pythonPlatform, microservice, microserviceToSpringBoot, microserviceToDotNet, customerMicroservice, shoppingCartMicroservice, orderMicroservice, microserviceToPython);
		log("### Changing microservice meta model and migrating Spring Boot generator manually:");
		repo.commit(microservice);
		repo.commit(copyArtifact(customerMicroservice)
			.updateMetamodel(microservice.version().increment()).build());
		repo.commit(copyArtifact(microserviceToSpringBoot)
			.updateDependency(microservice.version().increment()).build());
	}

	public static void example2() {
		repo.commit(executable, deploymentPipeline, sourceCode, ecore, trafoMM, java, javaBuildPipeline, springBootPlatform, dotNetPlatform, pythonPlatform, coEvModelGen, modelCoEvGen, trafoCoEvGen, microservice, microserviceToSpringBoot, microserviceToDotNet, customerMicroservice, shoppingCartMicroservice, orderMicroservice, microserviceToPython);
		// adding the meta model again will trigger the creation of a new version
		log("### Changing microservice meta model:");
		repo.commit(microservice);
	}

	public static void example3() {
		repo.commit(executable, deploymentPipeline, sourceCode, ecore, trafoMM, java, javaBuildPipeline, springBootPlatform, dotNetPlatform, pythonPlatform, coEvModelGen, modelCoEvGen, trafoCoEvGen, microservice, microserviceToSpringBoot, microserviceToDotNet, customerMicroservice, shoppingCartMicroservice, orderMicroservice, microserviceToPython);
		log("### Changing Spring Boot platform:");
		// update the platform
		repo.commit(springBootPlatform);
		// update the generator
		repo.commit(copyArtifact(microserviceToSpringBoot)
			.updateDependency(springBootPlatform.version().increment()).build());
	}

	public static void example4() {
		repo.commit(executable, deploymentPipeline, sourceCode, ecore, trafoMM, java, javaBuildPipeline, springBootPlatform, dotNetPlatform, pythonPlatform, coEvModelGen, modelCoEvGen, trafoCoEvGen, microservice, microserviceToSpringBoot, microserviceToDotNet, customerMicroservice, shoppingCartMicroservice, orderMicroservice, microserviceToPython);
		log("### Changing Java version and migrating Spring Boot platform, Java build pipeline and Spring Boot Generator manually:");
		// update java
		repo.commit(java);
		// manual co-evolution
		repo.commit(copyArtifact(springBootPlatform)
			.updateMetamodel(java.version().increment()).build());
		repo.commit(copyArtifact(javaBuildPipeline)
			.updateDependency(java.version().increment()).build());
		repo.commit(copyArtifact(microserviceToSpringBoot)
			.updateDependency(springBootPlatform.version().increment()).build());
	}
	
	public static void onChange(Repository repo, ArtifactVersion version) {
		for (ArtifactVersion metamodel : repo.getMetamodels(version)) {
			for (Transformation transformation : repo.getAcceptingTransformations(metamodel)) {
				transformation.getTransformation().apply(repo.get(version)).ifPresent(repo::commit);
			}
		}
		for (ArtifactVersion metamodel : repo.getInputs(version)) {
			for (Artifact model : repo.getInstances(metamodel)) {
				repo.get(version).asTransformation().map(Transformation::getTransformation)
					.flatMap(t -> t.apply(model)).ifPresent(repo::commit);
			}
		}
	}
	
	public static interface Artifact {
		
		ArtifactVersion version();
		
		Set<ArtifactVersion> getMetamodels();
		
		Set<ArtifactVersion> getInputs();
		
		Set<ArtifactVersion> getOutputs();
		
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
		private final Set<ArtifactVersion> inputs;
		private final Set<ArtifactVersion> outputs;
		
		public ArtifactImpl(ArtifactVersion version, Set<ArtifactVersion> metamodels, Set<ArtifactVersion> inputs, Set<ArtifactVersion> outputs) {
			this.version = version;
			this.metamodels = Collections.unmodifiableSet(metamodels);
			this.inputs = Collections.unmodifiableSet(inputs);
			this.outputs = Collections.unmodifiableSet(outputs);
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
		public Set<ArtifactVersion> getInputs() {
			return inputs;
		}
		
		@Override
		public Set<ArtifactVersion> getOutputs() {
			return outputs;
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
			if (debug) {
				return String.format("%s; metamodels=%s; inputs=%s; outputs=%s", version, metamodels, inputs, outputs);
			}
			return version.toString();
		}
		
	}
	
	public static class TransformationImpl extends ArtifactImpl implements Transformation {
		
		private final Function<Artifact, Optional<Artifact>> transformation;
		
		public TransformationImpl(ArtifactVersion version, Set<ArtifactVersion> metamodels, Set<ArtifactVersion> inputs, Set<ArtifactVersion> outputs, Function<Artifact, Optional<Artifact>> transformation) {
			super(version, metamodels, inputs, outputs);
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
		
		public CoEvolutionModelImpl(ArtifactVersion version, Set<ArtifactVersion> metamodels, Set<ArtifactVersion> inputs, Set<ArtifactVersion> outputs, ArtifactVersion changedArtifact) {
			super(version, metamodels, inputs, outputs);
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
			if (debug) {
				return String.format("%s; changedArtifact=%s", super.toString(), changedArtifact);
			}
			return super.toString();
		}
		
	}
	
	public interface Repository {

		Artifact get(ArtifactVersion version);

		Set<Artifact> getInstances(ArtifactVersion version);

		Set<ArtifactVersion> getMetamodels(ArtifactVersion version);

		Set<ArtifactVersion> getInputs(ArtifactVersion version);

		Set<Transformation> getAcceptingTransformations(ArtifactVersion version);

		void commit(Artifact a);

		void commit(Artifact... a);

	}
	
	public static class RepositoryImpl implements Repository {
		
		private Map<ArtifactVersion, Artifact> artifactsByVersion = new HashMap<>();
		
		@Override
		public Artifact get(ArtifactVersion version) {
			return artifactsByVersion.get(version);
		}
		
		@Override
		public Set<Artifact> getInstances(ArtifactVersion version) {
			return artifactsByVersion.values().stream()
				// find any model that has declared the argument as meta model
				.filter(m1 -> m1.getMetamodels().contains(version))
				.collect(Collectors.toSet());
		}
		
		@Override
		public Set<ArtifactVersion> getMetamodels(ArtifactVersion version) {
			return Optional.ofNullable(version)
				.map(artifactsByVersion::get)
				.map(Artifact::getMetamodels)
				.orElse(Collections.emptySet());
		}
		
		@Override
		public Set<ArtifactVersion> getInputs(ArtifactVersion version) {
			return Optional.ofNullable(version)
				.map(artifactsByVersion::get)
				.map(Artifact::getInputs)
				.orElse(Collections.emptySet());
		}
		
		@Override
		public Set<Transformation> getAcceptingTransformations(ArtifactVersion version) {
			return artifactsByVersion.values().stream().map(Artifact::asTransformation)
				// find any transformation that has declared the argument as an input
				.filter(Optional::isPresent)
				.map(Optional::get)
				.filter(t -> t.getInputs().contains(version))
				.collect(Collectors.toSet());
		}
		
		@Override
		public void commit(Artifact a) {
			ArtifactVersion version = a.version();
			while (artifactsByVersion.containsKey(version)) {
				version = version.increment();
			}			
			Artifact newVersion = copyArtifact(a).withVersion(version).build();
			artifactsByVersion.put(newVersion.version(), newVersion);
			log("[COMMIT] " + newVersion);
			onChange(this, newVersion.version());
		}
		
		@Override
		public void commit(Artifact... a) {
			Arrays.asList(a).forEach(this::commit);
		}
		
	}
	
	public abstract static class AbstractArtifactBuilder<T extends Artifact, U extends AbstractArtifactBuilder<T, U>> {
		
		protected ArtifactVersion version;
		
		protected final Set<ArtifactVersion> metamodels = new HashSet<>();
		
		protected final Set<ArtifactVersion> inputs = new HashSet<>();
		
		protected final Set<ArtifactVersion> outputs = new HashSet<>();
		
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
		
		public U withInput(ArtifactVersion input) {
			this.inputs.add(input);
			return getThis();
		}
		
		public U withOutput(ArtifactVersion output) {
			this.outputs.add(output);
			return getThis();
		}
		
		public U updateDependency(ArtifactVersion version) {
			if (this.inputs.remove(version.decrement())) {
				this.inputs.add(version);
			}
			if (this.outputs.remove(version.decrement())) {
				this.outputs.add(version);
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
			return new ArtifactImpl(version, metamodels, inputs, outputs);
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
			return new TransformationImpl(version, metamodels, inputs, outputs, transformation);
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
			return new CoEvolutionModelImpl(version, metamodels, inputs, outputs, changedArtifact);
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
		artifact.getInputs().forEach(builder::withInput);
		artifact.getOutputs().forEach(builder::withOutput);
		return builder;
	}
	
}
