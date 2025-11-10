"""Command-line interface for MetaFlow."""

import click
from pathlib import Path
from rich.console import Console
from rich.table import Table

from metaflow.core.metadata_extractor import MetadataExtractor
from metaflow.core.pipeline_generator import PipelineGenerator
from metaflow.utils.config import load_config, save_config
from metaflow.utils.logger import setup_logger

console = Console()
logger = setup_logger(__name__)


@click.group()
@click.version_option()
def main():
    """MetaFlow - Metadata-Driven ETL Pipelines"""
    pass


@main.command()
@click.option("--source", required=True, help="Source database connection string")
@click.option("--target", required=True, help="Target Delta Lake path")
@click.option("--tables", help="Comma-separated list of tables (optional)")
@click.option("--output", default="./config.yaml", help="Output config file path")
def init(source: str, target: str, tables: str, output: str):
    """Initialize MetaFlow project from database metadata."""
    
    console.print("[bold blue]🌊 MetaFlow Initialization[/bold blue]")
    console.print(f"Source: {source}")
    console.print(f"Target: {target}")
    
    try:
        # Extract metadata
        with console.status("[bold green]Extracting metadata..."):
            extractor = MetadataExtractor(source)
            metadata = extractor.extract(tables.split(",") if tables else None)
        
        console.print(f"[green]✓[/green] Found {len(metadata.tables)} tables")
        
        # Display table summary
        table = Table(title="Discovered Tables")
        table.add_column("Table", style="cyan")
        table.add_column("Columns", style="magenta")
        table.add_column("Primary Key", style="green")
        
        for tbl in metadata.tables[:10]:  # Show first 10
            pk = ", ".join(tbl.primary_keys) if tbl.primary_keys else "None"
            table.add_row(tbl.name, str(len(tbl.columns)), pk)
        
        console.print(table)
        
        if len(metadata.tables) > 10:
            console.print(f"[dim]... and {len(metadata.tables) - 10} more tables[/dim]")
        
        # Generate config
        config = {
            "source": {
                "type": extractor.db_type,
                "connection_string": source,
                "tables": [t.name for t in metadata.tables],
            },
            "target": {
                "type": "delta",
                "path": target,
                "partition_by": "created_year",
                "optimize": True,
            },
            "pipeline": {
                "deduplicate": True,
                "merge_mode": "upsert",
            },
        }
        
        # Save config
        save_config(config, output)
        console.print(f"[green]✓[/green] Configuration saved to {output}")
        
        console.print("\n[bold]Next steps:[/bold]")
        console.print("  1. Review config.yaml")
        console.print("  2. Run: [cyan]metaflow generate[/cyan]")
        console.print("  3. Run: [cyan]metaflow run batch[/cyan]")
        
    except Exception as e:
        console.print(f"[bold red]Error:[/bold red] {e}")
        raise click.Abort()


@main.command()
@click.option("--config", default="./config.yaml", help="Config file path")
@click.option("--output", default="./pipelines", help="Output directory")
def generate(config: str, output: str):
    """Generate pipeline code from configuration."""
    
    console.print("[bold blue]🔧 Generating Pipelines[/bold blue]")
    
    try:
        # Load config
        cfg = load_config(config)
        
        # Generate pipelines
        with console.status("[bold green]Generating code..."):
            generator = PipelineGenerator(cfg)
            files = generator.generate(output)
        
        console.print(f"[green]✓[/green] Generated {len(files)} files")
        
        for file in files:
            console.print(f"  📄 {file}")
        
        console.print("\n[bold]Run your pipeline:[/bold]")
        console.print(f"  [cyan]spark-submit {output}/batch_pipeline.py[/cyan]")
        
    except Exception as e:
        console.print(f"[bold red]Error:[/bold red] {e}")
        raise click.Abort()


@main.command()
@click.argument("mode", type=click.Choice(["batch", "stream"]))
@click.option("--config", default="./config.yaml", help="Config file path")
@click.option("--checkpoint", help="Checkpoint location for streaming")
def run(mode: str, config: str, checkpoint: str):
    """Run a MetaFlow pipeline."""
    
    console.print(f"[bold blue]🚀 Running {mode.title()} Pipeline[/bold blue]")
    
    try:
        cfg = load_config(config)
        
        if mode == "batch":
            from metaflow.core.pipeline_generator import BatchPipeline
            pipeline = BatchPipeline(cfg)
            pipeline.run()
        else:
            from metaflow.core.pipeline_generator import StreamingPipeline
            pipeline = StreamingPipeline(cfg, checkpoint)
            pipeline.run()
        
        console.print("[green]✓[/green] Pipeline completed successfully")
        
    except Exception as e:
        console.print(f"[bold red]Error:[/bold red] {e}")
        raise click.Abort()


@main.command()
@click.option("--config", default="./config.yaml", help="Config file path")
def validate(config: str):
    """Validate configuration file."""
    
    console.print("[bold blue]🔍 Validating Configuration[/bold blue]")
    
    try:
        cfg = load_config(config)
        
        # Validation checks
        checks = [
            ("Source configured", "source" in cfg),
            ("Target configured", "target" in cfg),
            ("Tables specified", bool(cfg.get("source", {}).get("tables"))),
        ]
        
        table = Table(title="Validation Results")
        table.add_column("Check", style="cyan")
        table.add_column("Status", style="green")
        
        all_passed = True
        for check_name, passed in checks:
            status = "✓ Pass" if passed else "✗ Fail"
            style = "green" if passed else "red"
            table.add_row(check_name, f"[{style}]{status}[/{style}]")
            all_passed = all_passed and passed
        
        console.print(table)
        
        if all_passed:
            console.print("\n[green]✓[/green] Configuration is valid")
        else:
            console.print("\n[red]✗[/red] Configuration has errors")
            raise click.Abort()
        
    except Exception as e:
        console.print(f"[bold red]Error:[/bold red] {e}")
        raise click.Abort()


if __name__ == "__main__":
    main()