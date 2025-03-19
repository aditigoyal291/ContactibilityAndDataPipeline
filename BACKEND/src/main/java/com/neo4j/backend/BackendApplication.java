package com.neo4j.backend;

import org.neo4j.driver.Driver;  // Changed import
import org.neo4j.driver.Session;  // Add this import
import org.neo4j.driver.Result;   // Add this import
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration;
import org.springframework.context.annotation.Bean;


@SpringBootApplication(exclude = {DataSourceAutoConfiguration.class})
public class BackendApplication {
	private static final Logger logger = LoggerFactory.getLogger(com.neo4j.backend.BackendApplication.class);

	private Driver driver;

	@Autowired
	public void setDriver(Driver driver) {
		this.driver = driver;
	}

	public static void main(String[] args) {
		SpringApplication.run(com.neo4j.backend.BackendApplication.class, args);
	}

	@Bean
	public CommandLineRunner commandLineRunner() {
		return args -> {
			logger.info("Verifying Neo4j connection...");
			try (Session session = driver.session()) {  // Use explicit Session type
				Result result = session.run("RETURN 1");  // Use explicit Result type
				result.consume();
				logger.info("Connected to Neo4j database successfully");
			} catch (Exception e) {
				logger.error("Database connection failed: {}", e.getMessage(), e);
			}
		};
	}
}