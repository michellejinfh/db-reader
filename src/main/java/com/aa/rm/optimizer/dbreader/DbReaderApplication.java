package com.aa.rm.optimizer.dbreader;

import com.aa.rm.optimizer.dbreader.service.FareLoadDBService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class DbReaderApplication implements CommandLineRunner {

    @Autowired
    FareLoadDBService fareLoadDBService;

    public static void main(String[] args) {
        SpringApplication.run(DbReaderApplication.class, args);
    }

    @Override
    public void run(String... args) throws Exception
    {
        fareLoadDBService.loadData(null);
        System.out.println("Completed...");
    }
}
