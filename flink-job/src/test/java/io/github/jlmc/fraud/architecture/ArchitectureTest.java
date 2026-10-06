package io.github.jlmc.fraud.architecture;

import com.tngtech.archunit.core.domain.JavaClasses;
import com.tngtech.archunit.core.importer.ClassFileImporter;
import com.tngtech.archunit.core.importer.ImportOption;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import static com.tngtech.archunit.lang.syntax.ArchRuleDefinition.noClasses;
import static com.tngtech.archunit.library.Architectures.layeredArchitecture;
import static com.tngtech.archunit.library.dependencies.SlicesRuleDefinition.slices;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Ports and adapters, enforced. Domain and application are plain Java: frameworks live in adapters only.
 */
class ArchitectureTest {

    private static final String BASE = "io.github.jlmc.fraud";

    private static JavaClasses classes;

    @BeforeAll
    static void importProductionClasses() {
        classes = new ClassFileImporter()
                .withImportOption(ImportOption.Predefined.DO_NOT_INCLUDE_TESTS)
                .importPackages(BASE);
    }

    @Test
    void productionCodeIsActuallyAnalysed() {
        // guards against a silently empty import (an architecture test that checks nothing is worse than none)
        assertThat(classes.contain(BASE + ".domain.history.CustomerHistory")).isTrue();
        assertThat(classes.contain(BASE + ".application.usecase.EvaluateRiskService")).isTrue();
    }

    @Test
    void dependenciesPointInwards() {
        layeredArchitecture()
                .consideringOnlyDependenciesInLayers()
                .layer("Domain").definedBy(BASE + ".domain..")
                .layer("Application").definedBy(BASE + ".application..")
                .layer("Adapters").definedBy(BASE + ".adapter..")
                .layer("Bootstrap").definedBy(BASE + ".bootstrap..")
                .whereLayer("Bootstrap").mayNotBeAccessedByAnyLayer()
                .whereLayer("Adapters").mayOnlyBeAccessedByLayers("Bootstrap")
                .whereLayer("Application").mayOnlyBeAccessedByLayers("Adapters", "Bootstrap")
                .whereLayer("Domain").mayOnlyBeAccessedByLayers("Application", "Adapters", "Bootstrap")
                .check(classes);
    }

    @Test
    void domainAndApplicationAreFrameworkFree() {
        noClasses()
                .that().resideInAnyPackage(BASE + ".domain..", BASE + ".application..")
                .should().dependOnClassesThat().resideInAnyPackage(
                        "org.apache.flink..", "org.apache.kafka..", "java.sql..", "javax.sql..", "org.postgresql..", "org.slf4j..")
                .because("the core must stay testable without Flink, Kafka or a database")
                .check(classes);
    }

    @Test
    void noClassReferencesAConcreteValidationRule() {
        noClasses()
                .that().resideInAPackage(BASE + "..")
                .should().dependOnClassesThat().resideInAPackage(BASE + ".validation.rules..")
                .because("rules are plugins discovered at runtime, never referenced by the job")
                .check(classes);
    }

    @Test
    void inboundAndOutboundAdaptersDoNotDependOnEachOther() {
        slices().matching(BASE + ".adapter.(*)..")
                .should().notDependOnEachOther()
                .because("only the bootstrap wires adapters together; they talk through application ports")
                .check(classes);
    }
}
