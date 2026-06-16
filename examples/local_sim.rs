use vsm_agent_kernel::transport::in_memory::InMemoryTransport;
use vsm_agent_kernel::*;

#[tokio::main]
async fn main() -> Result<()> {
    let root = ViableNode::new_root("root-codebase-vsm");
    let root_id = root.id.clone();

    let mut genome = OrganizationalGenome::from_root(root);

    let primary = ViableNode::new_leaf(
        "primary-code-service",
        LeafOperation::Coding {
            languages: vec!["rust".to_string()],
            domains: vec!["whole-codebase".to_string()],
        },
    );
    let primary_id = primary.id.clone();

    // Root has no operation, so it can contain System 1 children immediately.
    genome.apply_patch(GenomePatch::AddNode {
        parent_id: root_id.clone(),
        node: primary,
    })?;

    println!("Initial genome:");
    print_node(&genome, &root_id, 0)?;
    println!(
        "primary can write code? {}",
        genome.get_node(&primary_id)?.capabilities().can_write_code
    );

    // Promote the coding leaf into a recursive viable subsystem. Its former
    // coding operation is extracted into a child. The parent loses write access.
    genome.apply_patch(GenomePatch::PromoteLeafToMetasystem {
        node_id: primary_id.clone(),
        extracted_child_name: "generalist-coder".to_string(),
    })?;

    let reviewer = ViableNode::new_leaf(
        "test-reviewer",
        LeafOperation::Review {
            review_types: vec!["tests".to_string(), "diff-risk".to_string()],
        },
    );
    let reviewer_id = reviewer.id.clone();

    genome.apply_patch(GenomePatch::AddNode {
        parent_id: primary_id.clone(),
        node: reviewer,
    })?;

    genome.apply_patch(GenomePatch::AddRelation(RelationGene::new(
        genome.get_node(&primary_id)?.children[0].clone(),
        reviewer_id.clone(),
        RelationType::RequestsReview,
    )))?;

    println!("\nAfter mutation:");
    print_node(&genome, &root_id, 0)?;
    println!(
        "primary-code-service can write code? {}",
        genome.get_node(&primary_id)?.capabilities().can_write_code
    );

    let transport = InMemoryTransport::new();
    let kernel = Kernel::new(genome, transport);
    println!(
        "runtime check: primary-code-service can write code? {}",
        kernel.can_node_write_code(&primary_id).await?
    );

    Ok(())
}

fn print_node(genome: &OrganizationalGenome, node_id: &NodeId, indent: usize) -> Result<()> {
    let node = genome.get_node(node_id)?;
    let pad = " ".repeat(indent);
    println!(
        "{}- {} [{:?}] write_code={}",
        pad,
        node.name,
        node.mode(),
        node.capabilities().can_write_code
    );
    for child in &node.children {
        print_node(genome, child, indent + 2)?;
    }
    Ok(())
}
