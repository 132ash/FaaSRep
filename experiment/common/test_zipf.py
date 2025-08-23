import numpy as np
import matplotlib.pyplot as plt
from pathlib import Path # 导入 Path 对象，用于处理文件路径

class ZipfGenerator:
    """
    An efficient Zipf distribution sampler that supports all alpha >= 0.
    """
    def __init__(self, n, alpha):
        """
        Initializes the Zipf distribution sampler.
        :param n: Number of elements (from 1 to n).
        :param alpha: The parameter of the Zipf distribution (alpha >= 0).
        """
        if alpha < 0:
            raise ValueError("alpha must be greater than or equal to 0")
        
        self.n = n
        self.alpha = alpha
        
        if alpha == 0:
            # It's a uniform distribution if alpha=0
            self.probabilities = np.full(n, 1/n)
        else:
            # Calculate the weight for each element (1/k^alpha)
            weights = np.power(np.arange(1, n + 1, dtype=float), -alpha)
            # Normalize to get the probability distribution
            self.probabilities = weights / np.sum(weights)
        
        # Pre-calculate the cumulative distribution function (CDF)
        self.cdf = np.cumsum(self.probabilities)

    def sample(self, k=1):
        """
        Draws one or more samples from the Zipf distribution.
        Returns samples as 0-based indices.
        :param k: The number of samples to draw.
        :return: A numpy array of one or more samples.
        """
        # Generate k random values in the range [0, 1)
        random_values = np.random.rand(k)
        
        # Use searchsorted to find the position of random values in the CDF,
        # which is equivalent to inverse transform sampling.
        # This returns 0-based indices.
        samples = np.searchsorted(self.cdf, random_values)
        
        return samples if k > 1 else samples[0]


def test_and_plot_zipf_distribution():
    """
    Validates the ZipfGenerator and saves the distribution plot to a file.
    """
    # --- Test Parameters ---
    dataset_size = 10000
    alphas_to_test = [0.5, 0.75, 1.0, 1.25, 1.5]
    num_samples = 2_000_000  # Use a large number of samples for a smooth distribution

    # --- 关键修改：设置输出目录和文件名 ---
    script_dir = Path(__file__).parent
    output_dir = script_dir / 'test_fig'
    output_dir.mkdir(parents=True, exist_ok=True) # 创建文件夹，如果已存在则不报错
    output_filename = output_dir / 'zipf_distribution_validation.png'

    # Create a figure large enough to hold all subplots
    fig, axes = plt.subplots(len(alphas_to_test), 1, figsize=(8, 4 * len(alphas_to_test)))
    fig.suptitle('ZipfGenerator Distribution Validation (Log-Log Plot)', fontsize=16)

    for i, alpha in enumerate(alphas_to_test):
        print(f"Generating samples for alpha = {alpha}...")
        
        # 1. Initialize the ZipfGenerator
        zipf_sampler = ZipfGenerator(dataset_size, alpha)
        
        # 2. Generate a large number of samples
        samples = zipf_sampler.sample(k=num_samples)
        
        # 3. Count the occurrences of each element (rank)
        counts = np.bincount(samples, minlength=dataset_size)
        
        # 4. Prepare data for plotting
        ranks = np.arange(1, dataset_size + 1)
        non_zero_mask = counts > 0
        
        # 5. Plot on the corresponding subplot
        ax = axes[i]
        ax.loglog(ranks[non_zero_mask], counts[non_zero_mask], 'o', markersize=2, label=f'alpha = {alpha}')
        ax.set_title(f'Zipf Distribution with alpha = {alpha}')
        ax.set_xlabel('Rank (log scale)')
        ax.set_ylabel('Frequency (log scale)')
        ax.grid(True, which="both", ls="--", linewidth=0.5)
        ax.legend()

    print("Plotting complete.")
    plt.tight_layout(rect=[0, 0, 1, 0.97]) # Adjust layout to prevent title overlap
    
    # --- 关键修改：将 plt.show() 替换为 plt.savefig() ---
    plt.savefig(output_filename, dpi=300, bbox_inches='tight')
    print(f"Plot saved to: {output_filename}")


if __name__ == '__main__':
    test_and_plot_zipf_distribution()