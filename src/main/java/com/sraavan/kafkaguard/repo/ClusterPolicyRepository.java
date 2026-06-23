package com.sraavan.kafkaguard.repo;

import com.sraavan.kafkaguard.domain.ClusterPolicy;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.Optional;

public interface ClusterPolicyRepository extends JpaRepository<ClusterPolicy, Long> {
    Optional<ClusterPolicy> findByCluster(String cluster);
}
