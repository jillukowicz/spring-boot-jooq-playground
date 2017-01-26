package com.exploration.boot.jooq.playground;

import com.exploration.boot.jooq.playground.tables.SpaceTravel;
import com.exploration.boot.jooq.playground.tables.records.ResearchRecord;
import com.exploration.boot.jooq.playground.tables.records.SpaceTravelRecord;
import org.jooq.DSLContext;
import org.jooq.InsertValuesStep2;
import org.jooq.Query;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.util.List;
import java.util.Random;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static com.exploration.boot.jooq.playground.Tables.*;

@SpringBootApplication
public class BootJooqPlaygroundApplication implements CommandLineRunner {

  @Autowired
  private DSLContext context;

  public static void main(String[] args) {
    SpringApplication.run(BootJooqPlaygroundApplication.class, args);
  }

  @Override
  public void run(String... strings) throws Exception {
    this.executeAllTravels();
    this.analyzeCollectedResources();
    this.reportFindings();
  }

  private void executeAllTravels(){

    List<Query> spaceTravels = IntStream.iterate(1, id -> id++)
        .limit(100)
        .mapToObj(id -> this.context
            .insertInto(SPACE_TRAVEL)
            .columns(SPACE_TRAVEL.DESTINATION, SPACE_TRAVEL.DISTANCE)
            .values("ALPHA " + new Random().nextInt(100000), Long.valueOf(id * new Random().nextInt(100)))
        ).collect(Collectors.toList());

    this.context.batch(spaceTravels).execute();

  }

  private void analyzeCollectedResources() {


    IntStream.iterate(1, id -> id++)
        .limit(10)
        .forEach(id -> this.context
            .insertInto(SPACE_TRAVEL)
            .columns(SPACE_TRAVEL.DESTINATION, SPACE_TRAVEL.DISTANCE)
            .values("ALPHA " + new Random().nextInt(100000), Long.valueOf(id * new Random().nextInt(100)))
            .execute());


    this.context.select()
        .from(SPACE_TRAVEL)
        .stream()
        .forEach(travel -> {
          Long id = travel.into(SpaceTravel.SPACE_TRAVEL.ID).value1();
          Stream.of("lithium", "cobalt", "zinc", "europium")
              .forEach(resource -> this.context
                  .insertInto(RESEARCH)
                  .columns(RESEARCH.SPACE_TRAVEL_ID, RESEARCH.DESCRIPTION)
                  .values(id, resource)
                  .execute());
        });


  }

  private void reportFindings(){
    this.context
        .select()
        .from(SPACE_TRAVEL)
        .leftJoin(RESEARCH)
        .on(RESEARCH.SPACE_TRAVEL_ID.eq(SPACE_TRAVEL.ID))
        .stream()
        .forEach(finding -> System.out.println(finding));
  }




}
