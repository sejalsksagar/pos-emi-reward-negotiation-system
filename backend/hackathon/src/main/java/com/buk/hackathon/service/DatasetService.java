package com.buk.hackathon.service;


import jakarta.annotation.PostConstruct;
import org.springframework.stereotype.Service;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.*;

@Service
public class DatasetService {

    private final Map<String, Map<String, String>> userData = new HashMap<>();

    @PostConstruct
    public void loadData() {
        try {
        	System.out.println("Loading dataset...");
            BufferedReader reader = new BufferedReader(
                    new InputStreamReader(
                            getClass().getResourceAsStream("/data/ps3_pos_emi_reward_negotiation.csv")
                    )
            );

            String line;
            boolean header = true;

            while ((line = reader.readLine()) != null) {
                if (header) {
                    header = false;
                    continue;
                }

                String[] parts = line.split(",");

                String userId = parts[2]; // correct index

                Map<String, String> data = new HashMap<>();

                // Simulate credit score (not present in dataset)
                data.put("creditScore", String.valueOf(650 + new Random().nextInt(200))); // 650–850

                // Correct merchant category
                data.put("merchantCategory", parts[5]);

                // Simulate reward points
                data.put("rewardPoints", String.valueOf(new Random().nextInt(3000)));

                userData.put(userId, data);
                System.out.println("User data: " + data);
            }
            System.out.println("Loaded users: " + userData.keySet());

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public Map<String, String> getUserData(String userId) {
        return userData.getOrDefault(userId, new HashMap<>());
    }
}
